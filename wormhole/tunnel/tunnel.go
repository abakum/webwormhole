package tunnel

import (
	"context"
	"io"
	"log"
	"net"
	"sync"
)

type Tunnel struct {
	session *Session
	ready   chan struct{}
}

func NewTunnel(session *Session) *Tunnel {
	t := &Tunnel{
		session: session,
		ready:   make(chan struct{}),
	}
	close(t.ready)
	return t
}

func (t *Tunnel) Ready() <-chan struct{} {
	return t.ready
}

func (t *Tunnel) Dial(ctx context.Context, remoteAddr string) (net.Conn, error) {
	return t.session.Dial(ctx, remoteAddr)
}

func (t *Tunnel) Listen() (net.Listener, error) {
	return t.session.Listen(), nil
}

func (t *Tunnel) Forward(ctx context.Context, localAddr string) error {
	ln, err := net.Listen("tcp", localAddr)
	if err != nil {
		return err
	}
	defer ln.Close()

	log.Printf("tunnel forward: listening on %s", localAddr)

	go func() {
		select {
		case <-ctx.Done():
		case <-t.session.stopCh:
			// log.Printf("tunnel forward: session closed, shutting down")
		}
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-t.session.stopCh:
				return nil
			default:
				return err
			}
		}
		go func(localConn net.Conn) {
			defer localConn.Close()
			remoteAddr := localConn.RemoteAddr().String()
			// log.Printf("tunnel forward: accepted connection from %s, opening tunnel", remoteAddr)
			tunnelConn, err := t.Dial(ctx, remoteAddr)
			if err != nil {
				log.Printf("tunnel forward: failed to dial tunnel: %v", err)
				return
			}
			defer tunnelConn.Close()
			Proxy(localConn, tunnelConn)
			// log.Printf("tunnel forward: connection from %s closed", remoteAddr)
		}(conn)
	}
}

func (t *Tunnel) Serve(ctx context.Context, localAddr string) error {
	ln := t.session.Listen()
	defer ln.Close()

	log.Printf("tunnel serve: waiting for tunnel connections, target=%s", localAddr)

	go func() {
		select {
		case <-ctx.Done():
		case <-t.session.stopCh:
			// log.Printf("tunnel serve: session closed, shutting down")
		}
		ln.Close()
	}()

	for {
		tunnelConn, err := ln.Accept()
		if err != nil {
			select {
			case <-t.session.stopCh:
				return nil
			default:
				return err
			}
		}
		go func(tc net.Conn) {
			defer tc.Close()
			// log.Printf("tunnel serve: got tunnel connection from %s, dialing %s", tc.RemoteAddr(), localAddr)
			localConn, err := net.Dial("tcp", localAddr)
			if err != nil {
				log.Printf("tunnel serve: failed to dial %s: %v", localAddr, err)
				return
			}
			defer localConn.Close()
			// log.Printf("tunnel serve: connected to %s, proxying", localAddr)
			Proxy(localConn, tc)
			// log.Printf("tunnel serve: connection to %s closed", localAddr)
		}(tunnelConn)
	}
}

func (t *Tunnel) Close() error {
	return t.session.Close()
}

func Proxy(a, b net.Conn) {
	var once sync.Once
	done := make(chan struct{})

	go func() {
		io.Copy(a, b)
		once.Do(func() { close(done) })
	}()

	go func() {
		io.Copy(b, a)
		once.Do(func() { close(done) })
	}()

	<-done
	a.Close()
	b.Close()
}
