# План: Добавить туннель поверх WebRTC в webwormhole

## Контекст

**wormhole-william** (этот репо) имеет туннельный функционал:
- `wormhole/tunnel/` — транспортно-независимый пакет: `RecordIO` → `Session` → `Tunnel`
- `wormhole/tunnel_integration.go` — интеграция с mailbox+transit relay (PAKE → transit → cryptor → RecordIO → Session → Tunnel)
- `Tunnel` предоставляет `Dial`/`Listen`/`Forward`/`Serve` — мультиплексные виртуальные TCP-соединения поверх одного транспортного канала

**webwormhole** (`../webwormhole`) использует WebRTC DataChannels:
- `wormhole/dial.go` — `New()` и `Join()` возвращают `*Wormhole` (реализует `io.ReadWriteCloser`)
- WebRTC DataChannel через `DetachDataChannels()` сохраняет границы сообщений (SCTP)
- Свой signalling server (WebSocket, CPACE PAKE)

**crocson** (`../crocson`) использует `wormhole-william` для туннелей через `wormhole_tunnel.go`.

## Цель

Добавить в webwormhole аналогичный туннельный функционал: `RecordIO`-адаптер поверх WebRTC `Wormhole`, туннельная мультиплексная сессия, и API вида `NewTunnel`/`JoinTunnel`.

## Где работать

**В репо webwormhole** (`../webwormhole`). Туннельный пакет транспортно-независим и может быть скопирован, а адаптер и интеграция относятся к webwormhole.

## План

### Шаг 1: Скопировать `tunnel/` пакет в webwormhole

Скопировать из `wormhole-william/wormhole/tunnel/` в `webwormhole/wormhole/tunnel/`:

| Файл | Описание |
|------|----------|
| `protocol.go` | Кодирование/декодирование сообщений (MsgOpen/MsgData/MsgClose) |
| `session.go` | RecordIO → Session (мультиплексор виртуальных соединений) |
| `tunnel.go` | Tunnel: Dial/Listen/Forward/Serve/Close/Proxy |
| `protocol_test.go` | Тесты протокола |
| `session_test.go` | Тесты сессии (pipeRecordIO) |
| `tunnel_test.go` | Интеграционные тесты туннеля |

Пакет не имеет внешних зависимостей от wormhole-william — только стандартная библиотека Go. Копируется как есть.

### Шаг 2: Создать `RecordIO`-адаптер поверх WebRTC `Wormhole`

Новый файл `webwormhole/wormhole/tunnel.go`:

```go
package wormhole

import (
    "webwormhole.io/wormhole/tunnel"
)

type wormholeRecordIO struct {
    wh *Wormhole
}

func (r *wormholeRecordIO) ReadRecord() ([]byte, error) {
    buf := make([]byte, 256*1024) // SCTP message max ~256KB
    n, err := r.wh.Read(buf)
    if err != nil {
        return nil, err
    }
    return buf[:n], nil
}

func (r *wormholeRecordIO) WriteRecord(msg []byte) error {
    _, err := r.wh.Write(msg)
    return err
}

func (r *wormholeRecordIO) Close() error {
    return r.wh.Close()
}
```

WebRTC DataChannel через `DetachDataChannels()` сохраняет границы сообщений SCTP, поэтому framing не нужен — каждая `WriteRecord` = одно SCTP-сообщение, каждая `ReadRecord` = одно SCTP-сообщение.

### Шаг 3: Добавить `NewTunnel` и `JoinTunnel`

В том же `webwormhole/wormhole/tunnel.go`:

```go
func NewTunnel(pass string, sigserv string, slotc chan string) (*Tunnel, error) {
    c, err := New(pass, sigserv, slotc)
    if err != nil {
        return nil, err
    }
    session := tunnel.NewSession(&wormholeRecordIO{wh: c})
    return tunnel.NewTunnel(session), nil
}

func JoinTunnel(slot, pass string, sigserv string) (*Tunnel, error) {
    c, err := Join(slot, pass, sigserv)
    if err != nil {
        return nil, err
    }
    session := tunnel.NewSession(&wormholeRecordIO{wh: c})
    return tunnel.NewTunnel(session), nil
}
```

Здесь `Tunnel` — это `*tunnel.Tunnel` из скопированного пакета (или можно вернуть `tunnel.Tunnel` по значению, следуя стилю wormhole-william).

### Шаг 4: Обновить `go.mod`

Убедиться, что `go.mod` в webwormhole не требует новых зависимостей (туннельный пакет использует только stdlib).

### Шаг 5: Примеры использования

Создать примеры по аналогии с `wormhole-william/examples/`:

- `webwormhole/examples/ww-tunnel-dial/` — создаёт туннель, вызывает `Serve` (пробрасывает локальный TCP-сервер)
- `webwormhole/examples/ww-tunnel-bind/` — подключается к туннелю, вызывает `Forward` (биндит локальный порт)

### Шаг 6: Тесты

Существующие тесты из `tunnel/` (protocol, session, tunnel) будут работать без изменений — они используют `pipeRecordIO`, не зависят от WebRTC.

## Результирующая структура webwormhole

```
webwormhole/
  wormhole/
    dial.go              # существующий (New, Join)
    tunnel.go            # НОВЫЙ: RecordIO-адаптер + NewTunnel + JoinTunnel
    tunnel/
      protocol.go        # копия из wormhole-william
      session.go         # копия из wormhole-william
      tunnel.go          # копия из wormhole-william
      protocol_test.go   # копия из wormhole-william
      session_test.go    # копия из wormhole-william
      tunnel_test.go     # копия из wormhole-william
  examples/
    ww-tunnel-dial/      # НОВЫЙ
    ww-tunnel-bind/      # НОВЫЙ
```

## Использование из crocson (в будущем)

После реализации crocson сможет использовать webwormhole-туннель аналогично wormhole-william:

```go
import (
    ww "webwormhole.io/wormhole"
    "webwormhole.io/wormhole/tunnel"
)

func startWebWormholeSender(ctx context.Context, secret, sigserv string) (string, *tunnel.Tunnel, error) {
    pass := make([]byte, 2)
    rand.Read(pass)
    slotc := make(chan string)
    go func() { log.Println("code:", <-slotc) }()

    t, err := ww.NewTunnel(string(pass), sigserv, slotc)
    return code, t, err
}
```
