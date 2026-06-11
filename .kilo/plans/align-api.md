# Plan: Align webwormhole tunnel API with wormhole-william

## Goal

crocson's `wormhole_tunnel.go` uses wormhole-william. We want crocson to switch to webwormhole by changing only the import, with zero or minimal code changes. The examples should also match the wormhole-william examples structurally.

## crocson call sites (what must work)

```go
whClient := wh.Client{
    RendezvousURL:       ensureMailboxURL(mailboxURL),  // → SignalServer
    TransitRelayAddress: transitAddr,                    // → ignored (WebRTC)
}
code, connect, err := whClient.PrepareTunnel(ctx, secret)  // exact same signature
_, t, err := whClient.JoinTunnel(ctx, secret)               // exact same signature
t.Forward(ctx, addr)                                        // same (shared tunnel pkg)
t.Close()                                                   // same
```

## Design: `wormhole/client_ctx.go`

### Constants

```go
const DefaultSignalServer = "https://webwormhole.com"
```

### Client struct

```go
type Client struct {
    // SignalServer is the URL of the signalling server.
    // If empty, DefaultSignalServer is used.
    SignalServer string

    // TransitRelayAddress is ignored (webwormhole uses WebRTC).
    // Present for API compatibility with wormhole-william.
    TransitRelayAddress string
}
```

`TransitRelayAddress` field exists but is ignored — this allows crocson to set it without errors.

### Methods — identical signatures to wormhole-william

```go
func (c *Client) PrepareTunnel(ctx context.Context, code string) (string, func() (*tunnel.Tunnel, error), error)
func (c *Client) JoinTunnel(ctx context.Context, code string) (string, *tunnel.Tunnel, error)
```

### PrepareTunnel implementation

**When `code == ""` (new tunnel, creator role):**

Split `NewCtx` from `dial_ctx.go` into two phases:

Phase 1 — in `PrepareTunnel`:
1. Generate random 2-byte pass
2. Parse signal server URL
3. `websocket.Dial(ctx, wsaddr, ...)` — connect to signalling server
4. Start ctx-cancellation goroutine (`<-ctx.Done()` → close ws + pc)
5. `readInitMsgCtx(ctx, ws)` — get assigned slot + ICE servers
6. `c.newPeerConnection(iceServers)` — create WebRTC PeerConnection
7. Encode code: `wordlist.Encode(slot, pass)` → `generatedCode`
8. Return `(generatedCode, connect, nil)`

Phase 2 — in `connect()` closure (captures ctx, ws, wormhole, pass):
1. `readBase64Ctx(ctx, ws)` — read PAKE msg A (blocks until peer connects)
2. PAKE exchange
3. Send offer, read answer
4. Handle remote candidates
5. Wait for WebRTC DataChannel open
6. Wrap in `tunnel.NewSession(&wormholeRecordIOCtx{wh: c})` + `tunnel.NewTunnel(session)`
7. Return `*tunnel.Tunnel`

**When `code != ""` (join mode via PrepareTunnel):**
1. `wordlist.Decode(code)` → slot + pass
2. Return `(code, connect, nil)` where `connect()` calls `JoinTunnelCtx` internally

### JoinTunnel implementation

1. `wordlist.Decode(code)` → slot + pass
2. `JoinTunnelCtx(ctx, strconv.Itoa(slot), string(pass), c.signalServer())`
3. Return `(code, tunnel, error)`

## Files to create

1. **`wormhole/client_ctx.go`** — `Client`, `DefaultSignalServer`, `PrepareTunnel`, `JoinTunnel`, `signalServer()`

## Files to update

2. **`examples/ww-tunnel-dial/main.go`** — use `Client.PrepareTunnel`
3. **`examples/ww-tunnel-bind/main.go`** — use `Client.JoinTunnel`

## Files NOT touched

- `wormhole/dial.go` — original `New`, `Join`
- `wormhole/tunnel.go` — original `NewTunnel`, `JoinTunnel`
- `wormhole/dial_ctx.go` — `NewCtx`, `JoinCtx` (used internally by Client)
- `wormhole/tunnel_ctx.go` — `NewTunnelCtx`, `JoinTunnelCtx` (used internally by Client)
- `cmd/ww/main.go` — uses original API

## Updated examples

### ww-tunnel-dial — matches wormhole-william-tunnel-dial

```go
package main

import (
    "context"
    "flag"
    "fmt"
    "log"
    "net/http"
    "os/signal"
    "syscall"

    "webwormhole.io/wormhole"
)

func main() {
    code := flag.String("code", "", "wormhole code (leave empty to generate)")
    dial := flag.String("dial", ":8080", "local address to serve and expose via tunnel")
    sigserv := flag.String("signal", wormhole.DefaultSignalServer, "signalling server")
    flag.Parse()

    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        fmt.Fprintf(w, "hello from webwormhole tunnel\n")
    })
    srv := &http.Server{Addr: *dial}
    go srv.ListenAndServe()
    go func() { <-ctx.Done(); srv.Close() }()

    c := wormhole.Client{SignalServer: *sigserv}

    generatedCode, connect, err := c.PrepareTunnel(ctx, *code)
    if err != nil {
        log.Fatalf("prepare tunnel: %v", err)
    }
    log.Printf("wormhole code: %s", generatedCode)

    t, err := connect()
    if err != nil {
        log.Fatalf("connect tunnel: %v", err)
    }
    defer t.Close()
    log.Printf("tunnel created, serving %s", *dial)
    if err := t.Serve(ctx, *dial); err != nil && ctx.Err() == nil {
        log.Fatalf("dial: %v", err)
    }
}
```

### ww-tunnel-bind — matches wormhole-william-tunnel-bind

```go
package main

import (
    "context"
    "flag"
    "fmt"
    "io"
    "log"
    "net/http"
    "os/signal"
    "syscall"
    "time"

    "webwormhole.io/wormhole"
)

func main() {
    code := flag.String("code", "", "wormhole code (required)")
    bind := flag.String("bind", ":8081", "local address to bind")
    sigserv := flag.String("signal", wormhole.DefaultSignalServer, "signalling server")
    flag.Parse()
    if *code == "" {
        log.Fatalf("usage: ww-tunnel-bind -code <wormhole-code> [-bind addr]")
    }

    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    c := wormhole.Client{SignalServer: *sigserv}

    _, t, err := c.JoinTunnel(ctx, *code)
    if err != nil {
        log.Fatalf("join tunnel: %v", err)
    }
    defer t.Close()
    log.Printf("joined tunnel, binding %s", *bind)

    go func() {
        if err := t.Forward(ctx, *bind); err != nil && ctx.Err() == nil {
            log.Fatalf("forward: %v", err)
        }
    }()

    time.Sleep(500 * time.Millisecond)

    resp, err := http.Get(fmt.Sprintf("http://%s/", *bind))
    if err != nil {
        log.Fatalf("GET: %v", err)
    }
    body, err := io.ReadAll(resp.Body)
    resp.Body.Close()
    if err != nil {
        log.Fatalf("read body: %v", err)
    }
    log.Printf("GET response: %s", string(body))
}
```

## crocson migration impact

After this change, crocson's `wormhole_tunnel.go` changes:

```diff
- import wh "github.com/psanford/wormhole-william/wormhole"
- import "github.com/psanford/wormhole-william/wormhole/tunnel"
+ import wh "webwormhole.io/wormhole"
+ import "webwormhole.io/wormhole/tunnel"
```

```diff
  whClient := wh.Client{
-     RendezvousURL:       ensureMailboxURL(mailboxURL),
+     SignalServer:        ensureSignalServer(mailboxURL),
      TransitRelayAddress: transitAddr,
  }
```

Everything else (`PrepareTunnel`, `JoinTunnel`, `t.Forward`, `t.Close`) — identical call sites.

## Verification
1. `go build ./...`
2. Full chain test: dial → bind → GET response
3. CTRL-C during peer wait → exits within ~3s
4. Dial exits after bind disconnects
