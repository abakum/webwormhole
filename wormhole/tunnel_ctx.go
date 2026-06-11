package wormhole

import (
	"context"
	"time"

	"webwormhole.io/wormhole/tunnel"
)

type wormholeRecordIOCtx struct {
	wh *Wormhole
}

func (r *wormholeRecordIOCtx) ReadRecord() ([]byte, error) {
	buf := make([]byte, sctpMaxMessageSize)
	n, err := r.wh.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func (r *wormholeRecordIOCtx) WriteRecord(msg []byte) error {
	_, err := r.wh.Write(msg)
	return err
}

func (r *wormholeRecordIOCtx) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	return r.wh.CloseCtx(ctx)
}

func NewTunnelCtx(ctx context.Context, pass string, sigserv string, slotc chan string) (*tunnel.Tunnel, error) {
	c, err := NewCtx(ctx, pass, sigserv, slotc)
	if err != nil {
		return nil, err
	}
	session := tunnel.NewSession(&wormholeRecordIOCtx{wh: c})
	return tunnel.NewTunnel(session), nil
}

func JoinTunnelCtx(ctx context.Context, slot, pass string, sigserv string) (*tunnel.Tunnel, error) {
	c, err := JoinCtx(ctx, slot, pass, sigserv)
	if err != nil {
		return nil, err
	}
	session := tunnel.NewSession(&wormholeRecordIOCtx{wh: c})
	return tunnel.NewTunnel(session), nil
}
