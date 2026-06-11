package tunnel

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type RecordIO interface {
	ReadRecord() ([]byte, error)
	WriteRecord([]byte) error
	Close() error
}

type writeRequest struct {
	data []byte
	err  chan error
}

type Session struct {
	rw        RecordIO
	conns     sync.Map
	nextID    uint64
	stopCh    chan struct{}
	wg        sync.WaitGroup
	controlCh chan writeRequest
	dataCh    chan writeRequest

	listenerMu sync.Mutex
	listener   *tunnelListener
	closeOnce  sync.Once
}

func NewSession(rw RecordIO) *Session {
	s := &Session{
		rw:        rw,
		stopCh:    make(chan struct{}),
		controlCh: make(chan writeRequest, 64),
		dataCh:    make(chan writeRequest, 256),
		listener: &tunnelListener{
			session: nil,
			connCh:  make(chan *tunnelConn, 64),
			closed:  make(chan struct{}),
		},
	}
	s.listener.session = s
	s.wg.Add(2)
	go s.readLoop()
	go s.writeLoop()
	return s
}

func (s *Session) Dial(ctx context.Context, remoteAddr string) (net.Conn, error) {
	select {
	case <-s.stopCh:
		return nil, net.ErrClosed
	default:
	}

	id := atomic.AddUint64(&s.nextID, 1)
	conn := newTunnelConn(id, s)
	s.conns.Store(id, conn)

	// log.Printf("tunnel session: dialing %s (connID=%d)", remoteAddr, id)

	if err := s.sendControl(EncodeOpen(id, remoteAddr)); err != nil {
		s.conns.Delete(id)
		return nil, fmt.Errorf("tunnel dial: %w", err)
	}

	return conn, nil
}

func (s *Session) Listen() net.Listener {
	s.listenerMu.Lock()
	defer s.listenerMu.Unlock()
	if s.listener != nil {
		return s.listener
	}
	ln := &tunnelListener{
		session: s,
		connCh:  make(chan *tunnelConn, 64),
		closed:  make(chan struct{}),
	}
	s.listener = ln
	return ln
}

func (s *Session) Close() error {
	s.close()
	err := s.rw.Close()
	s.wg.Wait()
	return err
}

func (s *Session) close() {
	s.closeOnce.Do(func() {
		close(s.stopCh)
		s.closeAll()
	})
}

func (s *Session) closeAll() {
	s.conns.Range(func(_, v interface{}) bool {
		conn := v.(*tunnelConn)
		conn.remoteClose()
		return true
	})
}

func (s *Session) readLoop() {
	defer s.wg.Done()
	// log.Printf("tunnel session: read loop started")
	for {
		record, err := s.rw.ReadRecord()
		if err != nil {
			// log.Printf("tunnel session: read loop ended: %v", err)
			s.close()
			return
		}
		msg, err := Decode(record)
		if err != nil {
			log.Printf("tunnel session: decode error: %v", err)
			continue
		}
		switch msg.Type {
		case MsgOpen:
			s.handleOpen(msg)
		case MsgData:
			s.handleData(msg)
		case MsgClose:
			s.handleClose(msg)
		}
	}
}

func (s *Session) handleOpen(msg Message) {
	_ = string(msg.Payload)
	// log.Printf("tunnel session: open request connID=%d addr=%s", msg.ConnID, addr)

	conn := newTunnelConn(msg.ConnID, s)
	s.conns.Store(msg.ConnID, conn)

	s.listenerMu.Lock()
	ln := s.listener
	s.listenerMu.Unlock()

	select {
	case ln.connCh <- conn:
		// log.Printf("tunnel session: accepted tunnel connID=%d", msg.ConnID)
	case <-s.stopCh:
		s.sendClose(msg.ConnID)
		s.conns.Delete(msg.ConnID)
	default:
		// log.Printf("tunnel session: listener full, rejecting connID=%d", msg.ConnID)
		s.sendClose(msg.ConnID)
		s.conns.Delete(msg.ConnID)
	}
}

func (s *Session) handleData(msg Message) {
	val, ok := s.conns.Load(msg.ConnID)
	if !ok {
		return
	}
	conn := val.(*tunnelConn)
	conn.deliverData(msg.Payload)
}

func (s *Session) handleClose(msg Message) {
	// log.Printf("tunnel session: close connID=%d", msg.ConnID)
	val, ok := s.conns.Load(msg.ConnID)
	if !ok {
		return
	}
	conn := val.(*tunnelConn)
	conn.remoteClose()
}

func (s *Session) sendData(connID uint64, data []byte) error {
	return s.enqueueData(EncodeData(connID, data))
}

func (s *Session) sendClose(connID uint64) {
	s.sendControl(EncodeClose(connID))
}

func (s *Session) sendControl(msg []byte) error {
	req := writeRequest{data: msg, err: make(chan error, 1)}
	select {
	case s.controlCh <- req:
	case <-s.stopCh:
		return net.ErrClosed
	}
	select {
	case err := <-req.err:
		return err
	case <-s.stopCh:
		return net.ErrClosed
	}
}

func (s *Session) enqueueData(msg []byte) error {
	req := writeRequest{data: msg, err: make(chan error, 1)}
	select {
	case s.dataCh <- req:
	case <-s.stopCh:
		return net.ErrClosed
	}
	select {
	case err := <-req.err:
		return err
	case <-s.stopCh:
		return net.ErrClosed
	}
}

func (s *Session) writeLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.stopCh:
			return
		case req := <-s.controlCh:
			req.err <- s.rw.WriteRecord(req.data)
		default:
		}
		select {
		case <-s.stopCh:
			return
		case req := <-s.controlCh:
			req.err <- s.rw.WriteRecord(req.data)
		case req := <-s.dataCh:
			req.err <- s.rw.WriteRecord(req.data)
		}
	}
}

type tunnelConn struct {
	id      uint64
	session *Session
	readMu  sync.Mutex
	readBuf bytes.Buffer
	readCh  chan []byte
	closeCh chan struct{}
	once    sync.Once
	closed  int32
}

func newTunnelConn(id uint64, s *Session) *tunnelConn {
	return &tunnelConn{
		id:      id,
		session: s,
		readCh:  make(chan []byte, 64),
		closeCh: make(chan struct{}),
	}
}

func (c *tunnelConn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	for {
		if c.readBuf.Len() > 0 {
			return c.readBuf.Read(p)
		}

		select {
		case data := <-c.readCh:
			if len(data) == 0 {
				return 0, io.EOF
			}
			c.readBuf.Write(data)
		case <-c.closeCh:
			return 0, io.EOF
		}
	}
}

func (c *tunnelConn) Write(p []byte) (int, error) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return 0, net.ErrClosed
	}
	if err := c.session.sendData(c.id, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *tunnelConn) Close() error {
	c.once.Do(func() {
		atomic.StoreInt32(&c.closed, 1)
		close(c.closeCh)
		c.session.conns.Delete(c.id)
		c.session.sendClose(c.id)
	})
	return nil
}

func (c *tunnelConn) LocalAddr() net.Addr  { return &tunnelAddr{c.id} }
func (c *tunnelConn) RemoteAddr() net.Addr { return &tunnelAddr{c.id} }

func (c *tunnelConn) SetDeadline(t time.Time) error      { return nil }
func (c *tunnelConn) SetReadDeadline(t time.Time) error   { return nil }
func (c *tunnelConn) SetWriteDeadline(t time.Time) error  { return nil }

func (c *tunnelConn) deliverData(data []byte) {
	buf := make([]byte, len(data))
	copy(buf, data)
	select {
	case c.readCh <- buf:
	case <-c.closeCh:
	}
}

func (c *tunnelConn) remoteClose() {
	c.once.Do(func() {
		atomic.StoreInt32(&c.closed, 1)
		close(c.closeCh)
		c.session.conns.Delete(c.id)
	})
}

type tunnelAddr struct{ id uint64 }

func (a *tunnelAddr) Network() string { return "tunnel" }
func (a *tunnelAddr) String() string  { return fmt.Sprintf("tunnel:%d", a.id) }

type tunnelListener struct {
	session *Session
	connCh  chan *tunnelConn
	closed  chan struct{}
	once    sync.Once
}

func (l *tunnelListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.connCh:
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	case <-l.session.stopCh:
		return nil, net.ErrClosed
	}
}

func (l *tunnelListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *tunnelListener) Addr() net.Addr { return &tunnelAddr{} }
