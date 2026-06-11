package wormhole

import (
	"webwormhole.io/wormhole/tunnel"
)

const sctpMaxMessageSize = 64 * 1024

type wormholeRecordIO struct {
	wh *Wormhole
}

func (r *wormholeRecordIO) ReadRecord() ([]byte, error) {
	buf := make([]byte, sctpMaxMessageSize)
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

func NewTunnel(pass string, sigserv string, slotc chan string) (*tunnel.Tunnel, error) {
	c, err := New(pass, sigserv, slotc)
	if err != nil {
		return nil, err
	}
	session := tunnel.NewSession(&wormholeRecordIO{wh: c})
	return tunnel.NewTunnel(session), nil
}

func JoinTunnel(slot, pass string, sigserv string) (*tunnel.Tunnel, error) {
	c, err := Join(slot, pass, sigserv)
	if err != nil {
		return nil, err
	}
	session := tunnel.NewSession(&wormholeRecordIO{wh: c})
	return tunnel.NewTunnel(session), nil
}
