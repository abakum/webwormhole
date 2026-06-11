package wormhole

import (
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"net/url"
	"strconv"
	"sync"
	"time"

	"filippo.io/cpace"
	webrtc "github.com/pion/webrtc/v3"
	"golang.org/x/crypto/hkdf"
	"nhooyr.io/websocket"
	"webwormhole.io/wordlist"
	"webwormhole.io/wormhole/tunnel"
)

const DefaultSignalServer = "https://webwormhole.com"

type Client struct {
	SignalServer string
	TransitRelayAddress string
}

func (c *Client) signalServer() string {
	if c.SignalServer != "" {
		return c.SignalServer
	}
	return DefaultSignalServer
}

func signalWSURL(sigserv string) (string, error) {
	u, err := url.Parse(sigserv)
	if err != nil {
		return "", err
	}
	if u.Scheme == "http" || u.Scheme == "ws" {
		u.Scheme = "ws"
	} else {
		u.Scheme = "wss"
	}
	return u.String(), nil
}

func (c *Client) PrepareTunnel(ctx context.Context, code string) (string, func() (*tunnel.Tunnel, error), error) {
	if code != "" {
		slot, pass := wordlist.Decode(code)
		if pass == nil {
			return "", nil, errors.New("invalid wormhole code")
		}
		return code, func() (*tunnel.Tunnel, error) {
			return JoinTunnelCtx(ctx, strconv.Itoa(slot), string(pass), c.signalServer())
		}, nil
	}

	pass := make([]byte, 2)
	if _, err := io.ReadFull(crand.Reader, pass); err != nil {
		return "", nil, err
	}

	wh := &Wormhole{
		opened: make(chan struct{}),
		err:    make(chan error),
		flushc: sync.NewCond(&sync.Mutex{}),
	}

	wsaddr, err := signalWSURL(c.signalServer())
	if err != nil {
		return "", nil, err
	}

	ws, _, err := websocket.Dial(ctx, wsaddr, &websocket.DialOptions{
		Subprotocols: []string{Protocol},
	})
	if err != nil {
		return "", nil, err
	}

	go func() {
		<-ctx.Done()
		ws.Close(CloseWebRTCFailed, "cancelled")
		if wh.pc != nil {
			wh.pc.Close()
		}
	}()

	assignedSlot, iceServers, err := readInitMsgCtx(ctx, ws)
	if websocket.CloseStatus(err) == CloseWrongProto {
		return "", nil, ErrBadVersion
	}
	if err != nil {
		return "", nil, err
	}
	logf("connected to signalling server, got slot: %v", assignedSlot)

	err = wh.newPeerConnection(iceServers)
	if err != nil {
		return "", nil, err
	}

	slot, err := strconv.Atoi(assignedSlot)
	if err != nil {
		return "", nil, err
	}
	generatedCode := wordlist.Encode(slot, pass)

	connect := func() (*tunnel.Tunnel, error) {
		msgA, err := readBase64Ctx(ctx, ws)
		if err != nil {
			return nil, err
		}
		logf("got A pake msg (%v bytes)", len(msgA))

		msgB, mk, err := cpace.Exchange(string(pass), cpace.NewContextInfo("", "", nil), msgA)
		if err != nil {
			return nil, err
		}
		key := [32]byte{}
		_, err = io.ReadFull(hkdf.New(sha256.New, mk, nil, nil), key[:])
		if err != nil {
			return nil, err
		}
		err = writeBase64Ctx(ctx, ws, msgB)
		if err != nil {
			return nil, err
		}
		logf("have key, sent B pake msg (%v bytes)", len(msgB))

		wh.pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
			if candidate == nil {
				return
			}
			err := writeEncJSONCtx(ctx, ws, &key, candidate.ToJSON())
			if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
				return
			}
			if err != nil {
				logf("cannot send local candidate: %v", err)
				return
			}
			logf("sent new local candidate: %v", candidate.String())
		})

		offer, err := wh.pc.CreateOffer(nil)
		if err != nil {
			return nil, err
		}
		err = writeEncJSONCtx(ctx, ws, &key, offer)
		if err != nil {
			return nil, err
		}
		err = wh.pc.SetLocalDescription(offer)
		if err != nil {
			return nil, err
		}
		logf("sent offer")

		var answer webrtc.SessionDescription
		err = readEncJSONCtx(ctx, ws, &key, &answer)
		if websocket.CloseStatus(err) == CloseBadKey {
			return nil, ErrBadKey
		}
		if err != nil {
			return nil, err
		}
		err = wh.pc.SetRemoteDescription(answer)
		if err != nil {
			return nil, err
		}
		logf("got answer")

		go wh.handleRemoteCandidatesCtx(ctx, ws, &key)

		select {
		case <-wh.opened:
			relay := wh.IsRelay()
			logf("webrtc connection succeeded (relay: %v) closing signalling channel", relay)
			if relay {
				ws.Close(CloseWebRTCSuccessRelay, "")
			} else {
				ws.Close(CloseWebRTCSuccessDirect, "")
			}
		case err = <-wh.err:
			ws.Close(CloseWebRTCFailed, "")
		case <-time.After(30 * time.Second):
			err = ErrTimedOut
			ws.Close(CloseWebRTCFailed, "timed out")
		case <-ctx.Done():
			err = ctx.Err()
		}
		if err != nil {
			return nil, err
		}

		session := tunnel.NewSession(&wormholeRecordIOCtx{wh: wh})
		return tunnel.NewTunnel(session), nil
	}

	return generatedCode, connect, nil
}

func (c *Client) JoinTunnel(ctx context.Context, code string) (string, *tunnel.Tunnel, error) {
	slot, pass := wordlist.Decode(code)
	if pass == nil {
		return "", nil, errors.New("invalid wormhole code")
	}
	t, err := JoinTunnelCtx(ctx, strconv.Itoa(slot), string(pass), c.signalServer())
	if err != nil {
		return "", nil, err
	}
	return code, t, nil
}
