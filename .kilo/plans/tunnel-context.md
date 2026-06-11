# Plan: Add context-aware tunnel functions

## Current state
- `wormhole/dial.go` — **modified**: my broken `Close()` fix with `goto close` + 5s deadline. **Must revert to original**.
- `examples/` — **untracked** (new directory, not in git). Can be rewritten freely.
- Everything else — clean.

## Step 0: Revert `wormhole/dial.go`
```bash
git checkout wormhole/dial.go
```

## Step 1: Create `wormhole/dial_ctx.go`

Context-aware versions of `New()` / `Join()` + all internal helpers. Same package, new file.

### Exported:
```go
func NewCtx(ctx context.Context, pass, sigserv string, slotc chan string) (*Wormhole, error)
func JoinCtx(ctx context.Context, slot, pass, sigserv string) (*Wormhole, error)
```

### Internal (unexported) — ctx-variants of helpers from `dial.go`:
```go
func readEncJSONCtx(ctx context.Context, ws *websocket.Conn, key *[32]byte, v interface{}) error
func writeEncJSONCtx(ctx context.Context, ws *websocket.Conn, key *[32]byte, v interface{}) error
func readBase64Ctx(ctx context.Context, ws *websocket.Conn) ([]byte, error)
func writeBase64Ctx(ctx context.Context, ws *websocket.Conn, p []byte) error
func readInitMsgCtx(ctx context.Context, ws *websocket.Conn) (string, []webrtc.ICEServer, error)
func (c *Wormhole) handleRemoteCandidatesCtx(ctx context.Context, ws *websocket.Conn, key *[32]byte)
func (c *Wormhole) CloseCtx(ctx context.Context) error
```

### Key differences from originals:
- `ctx` passed to all `ws.Read(ctx,...)` / `ws.Write(ctx,...)` — cancellation propagates to WebSocket reads/writes
- Final `select` in `NewCtx`/`JoinCtx` adds `<-ctx.Done()` case alongside `c.opened`/`c.err`/`time.After(30s)`
- `CloseCtx(ctx)` replaces the infinite `for { time.Sleep }` loop in `Close()` with a select on `ctx.Done()` + 5s hard deadline
- `handleRemoteCandidatesCtx` checks `ctx.Done()` on each iteration

## Step 2: Create `wormhole/tunnel_ctx.go`

```go
func NewTunnelCtx(ctx context.Context, pass, sigserv string, slotc chan string) (*tunnel.Tunnel, error)
func JoinTunnelCtx(ctx context.Context, slot, pass, sigserv string) (*tunnel.Tunnel, error)
```

Calls `NewCtx`/`JoinCtx`, wraps result in `tunnel.NewSession` + `tunnel.NewTunnel` (same pattern as existing `NewTunnel`/`JoinTunnel` in `tunnel.go`).

## Step 3: Rewrite `examples/ww-tunnel-dial/main.go`

```go
func main() {
    // flags...
    ctx, cancel := signal.NotifyContext(...)
    defer cancel()

    // http server...

    if *code != "" {
        slot, pass := wordlist.Decode(*code)
        t, err := wormhole.JoinTunnelCtx(ctx, strconv.Itoa(slot), string(pass), *sigserv)
        // ...
        t.Serve(ctx, *dial)
        return
    }

    // New tunnel
    pass := make([]byte, 2)
    io.ReadFull(crand.Reader, pass)

    slotc := make(chan string)
    go func() {
        s := <-slotc
        slot, _ := strconv.Atoi(s)
        log.Printf("wormhole code: %s", wordlist.Encode(slot, pass))
    }()

    t, err := wormhole.NewTunnelCtx(ctx, string(pass), *sigserv, slotc)
    if err != nil {
        if ctx.Err() != nil {
            return  // cancelled, clean exit
        }
        log.Fatalf("new tunnel: %v", err)
    }
    defer t.Close()
    t.Serve(ctx, *dial)
}
```

No `os.Exit`, no goroutine workarounds — `NewTunnelCtx` itself respects ctx.

## Step 4: Rewrite `examples/ww-tunnel-bind/main.go`

Same pattern — replace `wormhole.JoinTunnel(...)` with `wormhole.JoinTunnelCtx(ctx, ...)`.

## Files NOT touched
- `wormhole/dial.go` — revert to original, no other changes
- `wormhole/tunnel.go` — unchanged
- `cmd/ww/main.go` — unchanged (uses original `New()`/`Join()`)
- `wormhole/tunnel/` — unchanged (`Serve`/`Forward` already accept ctx)

## Verification
1. `go build ./...` — compiles
2. `go vet ./...` — no issues  
3. Run chain: `ww-tunnel-dial` → `ww-tunnel-bind`, verify GET response
4. CTRL-C during peer wait → exits within ~1s
5. CTRL-C after tunnel created → exits cleanly
