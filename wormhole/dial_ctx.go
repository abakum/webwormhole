package wormhole

import (
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/url"
	"sync"
	"time"

	"filippo.io/cpace"
	webrtc "github.com/pion/webrtc/v3"
	"golang.org/x/crypto/hkdf"
	"golang.org/x/crypto/nacl/secretbox"
	"nhooyr.io/websocket"
)

func readEncJSONCtx(ctx context.Context, ws *websocket.Conn, key *[32]byte, v interface{}) error {
	_, buf, err := ws.Read(ctx)
	if err != nil {
		return err
	}
	encrypted, err := base64.URLEncoding.DecodeString(string(buf))
	if err != nil {
		return err
	}
	var nonce [24]byte
	copy(nonce[:], encrypted[:24])
	jsonmsg, ok := secretbox.Open(nil, encrypted[24:], &nonce, key)
	if !ok {
		return ErrBadKey
	}
	return json.Unmarshal(jsonmsg, v)
}

func writeEncJSONCtx(ctx context.Context, ws *websocket.Conn, key *[32]byte, v interface{}) error {
	jsonmsg, err := json.Marshal(v)
	if err != nil {
		return err
	}
	var nonce [24]byte
	if _, err := io.ReadFull(crand.Reader, nonce[:]); err != nil {
		return err
	}
	return ws.Write(
		ctx,
		websocket.MessageText,
		[]byte(base64.URLEncoding.EncodeToString(
			secretbox.Seal(nonce[:], jsonmsg, &nonce, key),
		)),
	)
}

func readBase64Ctx(ctx context.Context, ws *websocket.Conn) ([]byte, error) {
	_, buf, err := ws.Read(ctx)
	if err != nil {
		return nil, err
	}
	return base64.URLEncoding.DecodeString(string(buf))
}

func writeBase64Ctx(ctx context.Context, ws *websocket.Conn, p []byte) error {
	return ws.Write(
		ctx,
		websocket.MessageText,
		[]byte(base64.URLEncoding.EncodeToString(p)),
	)
}

func readInitMsgCtx(ctx context.Context, ws *websocket.Conn) (string, []webrtc.ICEServer, error) {
	msg := struct {
		Slot       string             `json:"slot",omitempty`
		ICEServers []webrtc.ICEServer `json:"iceServers",omitempty`
	}{}

	_, buf, err := ws.Read(ctx)
	if err != nil {
		return "", nil, err
	}
	err = json.Unmarshal(buf, &msg)
	return msg.Slot, msg.ICEServers, err
}

func (c *Wormhole) handleRemoteCandidatesCtx(ctx context.Context, ws *websocket.Conn, key *[32]byte) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		var candidate webrtc.ICECandidateInit
		err := readEncJSONCtx(ctx, ws, key, &candidate)
		if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
			return
		}
		if err != nil {
			logf("cannot read remote candidate: %v", err)
			return
		}
		logf("received new remote candidate: %v", candidate.Candidate)
		err = c.pc.AddICECandidate(candidate)
		if err != nil {
			logf("cannot add candidate: %v", err)
			return
		}
	}
}

// CloseCtx attempts to flush the DataChannel buffers then close it
// and its PeerConnection. It respects the context for cancellation.
func (c *Wormhole) CloseCtx(ctx context.Context) (err error) {
	logf("closing")
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for c.d.BufferedAmount() != 0 {
		select {
		case <-ctx.Done():
			goto close
		case <-deadline.C:
			logf("close: timed out waiting for buffer flush, forcing close")
			goto close
		default:
		}
		time.Sleep(time.Second)
	}
close:
	tryclose := func(closer io.Closer) {
		e := closer.Close()
		if e != nil && err == nil {
			err = e
		}
	}
	defer tryclose(c.pc)
	defer tryclose(c.d)
	defer tryclose(c.rwc)
	return nil
}

// NewCtx is a context-aware version of New. It starts a new signalling
// handshake after asking the server to allocate a new slot. The context
// can be used to cancel the handshake at any stage.
func NewCtx(ctx context.Context, pass string, sigserv string, slotc chan string) (*Wormhole, error) {
	c := &Wormhole{
		opened: make(chan struct{}),
		err:    make(chan error),
		flushc: sync.NewCond(&sync.Mutex{}),
	}

	u, err := url.Parse(sigserv)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "http" || u.Scheme == "ws" {
		u.Scheme = "ws"
	} else {
		u.Scheme = "wss"
	}
	wsaddr := u.String()

	ws, _, err := websocket.Dial(ctx, wsaddr, &websocket.DialOptions{
		Subprotocols: []string{Protocol},
	})
	if err != nil {
		return nil, err
	}

	go func() {
		<-ctx.Done()
		ws.Close(CloseWebRTCFailed, "cancelled")
		if c.pc != nil {
			c.pc.Close()
		}
	}()

	assignedSlot, iceServers, err := readInitMsgCtx(ctx, ws)
	if websocket.CloseStatus(err) == CloseWrongProto {
		return nil, ErrBadVersion
	}
	if err != nil {
		return nil, err
	}
	logf("connected to signalling server, got slot: %v", assignedSlot)
	slotc <- assignedSlot
	err = c.newPeerConnection(iceServers)
	if err != nil {
		return nil, err
	}

	msgA, err := readBase64Ctx(ctx, ws)
	if err != nil {
		return nil, err
	}
	logf("got A pake msg (%v bytes)", len(msgA))

	msgB, mk, err := cpace.Exchange(pass, cpace.NewContextInfo("", "", nil), msgA)
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

	c.pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
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

	offer, err := c.pc.CreateOffer(nil)
	if err != nil {
		return nil, err
	}
	err = writeEncJSONCtx(ctx, ws, &key, offer)
	if err != nil {
		return nil, err
	}
	err = c.pc.SetLocalDescription(offer)
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
	err = c.pc.SetRemoteDescription(answer)
	if err != nil {
		return nil, err
	}
	logf("got answer")

	go c.handleRemoteCandidatesCtx(ctx, ws, &key)

	select {
	case <-c.opened:
		relay := c.IsRelay()
		logf("webrtc connection succeeded (relay: %v) closing signalling channel", relay)
		if relay {
			ws.Close(CloseWebRTCSuccessRelay, "")
		} else {
			ws.Close(CloseWebRTCSuccessDirect, "")
		}
	case err = <-c.err:
		ws.Close(CloseWebRTCFailed, "")
	case <-time.After(30 * time.Second):
		err = ErrTimedOut
		ws.Close(CloseWebRTCFailed, "timed out")
	case <-ctx.Done():
		err = ctx.Err()
	}
	return c, err
}

// JoinCtx is a context-aware version of Join. It performs the signalling
// handshake to join an existing slot. The context can be used to cancel
// the handshake at any stage.
func JoinCtx(ctx context.Context, slot, pass string, sigserv string) (*Wormhole, error) {
	c := &Wormhole{
		opened: make(chan struct{}),
		err:    make(chan error),
		flushc: sync.NewCond(&sync.Mutex{}),
	}

	u, err := url.Parse(sigserv)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "http" || u.Scheme == "ws" {
		u.Scheme = "ws"
	} else {
		u.Scheme = "wss"
	}
	u.Path += slot
	wsaddr := u.String()

	ws, _, err := websocket.Dial(ctx, wsaddr, &websocket.DialOptions{
		Subprotocols: []string{Protocol},
	})
	if err != nil {
		return nil, err
	}

	go func() {
		<-ctx.Done()
		ws.Close(CloseWebRTCFailed, "cancelled")
		if c.pc != nil {
			c.pc.Close()
		}
	}()

	_, iceServers, err := readInitMsgCtx(ctx, ws)
	if websocket.CloseStatus(err) == CloseWrongProto {
		return nil, ErrBadVersion
	}
	if err != nil {
		return nil, err
	}
	logf("connected to signalling server on slot: %v", slot)
	err = c.newPeerConnection(iceServers)
	if err != nil {
		return nil, err
	}

	msgA, pake, err := cpace.Start(pass, cpace.NewContextInfo("", "", nil))
	if err != nil {
		return nil, err
	}
	err = writeBase64Ctx(ctx, ws, msgA)
	if err != nil {
		return nil, err
	}
	logf("sent A pake msg (%v bytes)", len(msgA))

	msgB, err := readBase64Ctx(ctx, ws)
	if websocket.CloseStatus(err) == CloseWrongProto {
		return nil, ErrBadVersion
	}
	if err != nil {
		return nil, err
	}
	mk, err := pake.Finish(msgB)
	if err != nil {
		return nil, err
	}
	key := [32]byte{}
	_, err = io.ReadFull(hkdf.New(sha256.New, mk, nil, nil), key[:])
	if err != nil {
		return nil, err
	}
	logf("have key, got B msg (%v bytes)", len(msgB))

	var offer webrtc.SessionDescription
	err = readEncJSONCtx(ctx, ws, &key, &offer)
	if err == ErrBadKey {
		ws.Close(CloseBadKey, "bad key")
		return nil, err
	}
	if err != nil {
		return nil, err
	}

	c.pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
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

	err = c.pc.SetRemoteDescription(offer)
	if err != nil {
		return nil, err
	}
	logf("got offer")
	answer, err := c.pc.CreateAnswer(nil)
	if err != nil {
		return nil, err
	}
	err = writeEncJSONCtx(ctx, ws, &key, answer)
	if err != nil {
		return nil, err
	}
	err = c.pc.SetLocalDescription(answer)
	if err != nil {
		return nil, err
	}
	logf("sent answer")

	go c.handleRemoteCandidatesCtx(ctx, ws, &key)

	select {
	case <-c.opened:
		relay := c.IsRelay()
		logf("webrtc connection succeeded (relay: %v) closing signalling channel", relay)
		if relay {
			ws.Close(CloseWebRTCSuccessRelay, "")
		} else {
			ws.Close(CloseWebRTCSuccessDirect, "")
		}
	case err = <-c.err:
		ws.Close(CloseWebRTCFailed, "")
	case <-time.After(30 * time.Second):
		err = ErrTimedOut
		ws.Close(CloseWebRTCFailed, "timed out")
	case <-ctx.Done():
		err = ctx.Err()
	}
	return c, err
}
