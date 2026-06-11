package tunnel

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type pipeRecordIO struct {
	conn net.Conn
}

func newPipeRecordIOPair() (RecordIO, RecordIO) {
	a, b := net.Pipe()
	return &pipeRecordIO{conn: a}, &pipeRecordIO{conn: b}
}

func (p *pipeRecordIO) ReadRecord() ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(p.conn, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	buf := make([]byte, n)
	if _, err := io.ReadFull(p.conn, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (p *pipeRecordIO) WriteRecord(msg []byte) error {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(msg)))
	if _, err := p.conn.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := p.conn.Write(msg)
	return err
}

func (p *pipeRecordIO) Close() error {
	return p.conn.Close()
}

func TestSessionDialAccept(t *testing.T) {
	aIO, bIO := newPipeRecordIOPair()
	defer aIO.Close()
	defer bIO.Close()

	sessA := NewSession(aIO)
	sessB := NewSession(bIO)
	defer sessA.Close()
	defer sessB.Close()

	ln := sessB.Listen()

	connA, err := sessA.Dial(context.Background(), "test-addr")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer connA.Close()

	connB, err := ln.Accept()
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	defer connB.Close()
}

func TestSessionDataRoundTrip(t *testing.T) {
	aIO, bIO := newPipeRecordIOPair()
	defer aIO.Close()
	defer bIO.Close()

	sessA := NewSession(aIO)
	sessB := NewSession(bIO)
	defer sessA.Close()
	defer sessB.Close()

	ln := sessB.Listen()

	connA, err := sessA.Dial(context.Background(), "test-addr")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer connA.Close()

	connB, err := ln.Accept()
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	defer connB.Close()

	msgAtoB := []byte("hello from A to B")
	if _, err := connA.Write(msgAtoB); err != nil {
		t.Fatalf("Write A: %v", err)
	}
	buf := make([]byte, 64)
	n, err := connB.Read(buf)
	if err != nil {
		t.Fatalf("Read B: %v", err)
	}
	if string(buf[:n]) != string(msgAtoB) {
		t.Fatalf("A→B: expected %q, got %q", msgAtoB, buf[:n])
	}

	msgBtoA := []byte("hello from B to A")
	if _, err := connB.Write(msgBtoA); err != nil {
		t.Fatalf("Write B: %v", err)
	}
	n, err = connA.Read(buf)
	if err != nil {
		t.Fatalf("Read A: %v", err)
	}
	if string(buf[:n]) != string(msgBtoA) {
		t.Fatalf("B→A: expected %q, got %q", msgBtoA, buf[:n])
	}
}

func TestSessionMultipleConns(t *testing.T) {
	aIO, bIO := newPipeRecordIOPair()
	defer aIO.Close()
	defer bIO.Close()

	sessA := NewSession(aIO)
	sessB := NewSession(bIO)
	defer sessA.Close()
	defer sessB.Close()

	ln := sessB.Listen()

	const numConns = 5
	var connsA, connsB []net.Conn

	for i := 0; i < numConns; i++ {
		cA, err := sessA.Dial(context.Background(), "multi-addr")
		if err != nil {
			t.Fatalf("Dial %d: %v", i, err)
		}
		connsA = append(connsA, cA)

		cB, err := ln.Accept()
		if err != nil {
			t.Fatalf("Accept %d: %v", i, err)
		}
		connsB = append(connsB, cB)
	}

	var wg sync.WaitGroup
	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			msg := []byte("payload")
			connsA[idx].Write(msg)

			buf := make([]byte, 64)
			n, err := connsB[idx].Read(buf)
			if err != nil {
				t.Errorf("read conn %d: %v", idx, err)
				return
			}
			if string(buf[:n]) != string(msg) {
				t.Errorf("conn %d: expected %q, got %q", idx, msg, buf[:n])
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < numConns; i++ {
		connsA[i].Close()
		connsB[i].Close()
	}
}

func TestSessionClosePropagation(t *testing.T) {
	aIO, bIO := newPipeRecordIOPair()
	defer aIO.Close()
	defer bIO.Close()

	sessA := NewSession(aIO)
	sessB := NewSession(bIO)
	defer sessA.Close()
	defer sessB.Close()

	ln := sessB.Listen()

	connA, err := sessA.Dial(context.Background(), "close-test")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	connB, err := ln.Accept()
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}

	if err := connA.Close(); err != nil {
		t.Fatalf("Close A: %v", err)
	}

	buf := make([]byte, 64)
	_, err = connB.Read(buf)
	if err == nil {
		t.Fatal("expected EOF after remote close")
	}
	connB.Close()
}

func TestSessionNoListenerRejectsConn(t *testing.T) {
	aIO, bIO := newPipeRecordIOPair()
	defer aIO.Close()
	defer bIO.Close()

	sessA := NewSession(aIO)
	sessB := NewSession(bIO)
	defer sessA.Close()

	const numConns = 70
	var connsA []net.Conn
	for i := 0; i < numConns; i++ {
		cA, err := sessA.Dial(context.Background(), "overflow-test")
		if err != nil {
			t.Fatalf("Dial %d: %v", i, err)
		}
		connsA = append(connsA, cA)
	}

	sessB.Close()

	buf := make([]byte, 64)
	done := make(chan struct{})
	go func() {
		defer close(done)
		connsA[0].Read(buf)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("expected connection to be rejected (EOF) but timed out")
	}
	for _, c := range connsA {
		c.Close()
	}
}

type slowRecordIO struct {
	inner    RecordIO
	writeMu  sync.Mutex
	written  [][]byte
	writtenMu sync.Mutex
	delay    time.Duration
}

func (s *slowRecordIO) ReadRecord() ([]byte, error) {
	return s.inner.ReadRecord()
}

func (s *slowRecordIO) WriteRecord(msg []byte) error {
	s.writtenMu.Lock()
	s.written = append(s.written, append([]byte(nil), msg...))
	s.writtenMu.Unlock()
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return s.inner.WriteRecord(msg)
}

func (s *slowRecordIO) Close() error {
	return s.inner.Close()
}

func (s *slowRecordIO) getWritten() [][]byte {
	s.writtenMu.Lock()
	defer s.writtenMu.Unlock()
	out := make([][]byte, len(s.written))
	copy(out, s.written)
	return out
}

func TestSessionControlPriority(t *testing.T) {
	innerA, innerB := newPipeRecordIOPair()

	slowA := &slowRecordIO{inner: innerA, delay: time.Millisecond}
	slowB := &slowRecordIO{inner: innerB}

	sessA := NewSession(slowA)
	sessB := NewSession(slowB)
	defer sessA.Close()
	defer sessB.Close()

	ln := sessB.Listen()

	connA, err := sessA.Dial(context.Background(), "priority-test")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	connB, err := ln.Accept()
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}

	const numDataMsgs = 20
	var dataWg sync.WaitGroup
	dataWg.Add(numDataMsgs)
	for i := 0; i < numDataMsgs; i++ {
		go func() {
			defer dataWg.Done()
			connA.Write([]byte("data-payload"))
		}()
	}

	dataWg.Wait()

	closeStart := time.Now()
	connA.Close()
	closeElapsed := time.Since(closeStart)

	if closeElapsed > 100*time.Millisecond {
		t.Fatalf("Close took %v, expected near-instant (control priority)", closeElapsed)
	}

	for {
		buf := make([]byte, 64)
		_, err := connB.Read(buf)
		if err != nil {
			break
		}
	}
	connB.Close()

	written := slowA.getWritten()

	closeIdx := -1
	for i, msg := range written {
		if len(msg) > 0 && msg[0] == MsgClose {
			closeIdx = i
			break
		}
	}
	if closeIdx < 0 {
		t.Fatal("MsgClose not found in written messages")
	}

	dataAfterClose := 0
	for i := closeIdx + 1; i < len(written); i++ {
		if len(written[i]) > 0 && written[i][0] == MsgData {
			dataAfterClose++
		}
	}
	if dataAfterClose > 0 {
		t.Fatalf("found %d data messages after MsgClose — priority not working", dataAfterClose)
	}
}

func TestSessionConcurrentControlAndData(t *testing.T) {
	aIO, bIO := newPipeRecordIOPair()
	defer aIO.Close()
	defer bIO.Close()

	sessA := NewSession(aIO)
	sessB := NewSession(bIO)
	defer sessA.Close()
	defer sessB.Close()

	ln := sessB.Listen()

	const numConns = 10
	var connsA []net.Conn

	for i := 0; i < numConns; i++ {
		cA, err := sessA.Dial(context.Background(), "concurrent-test")
		if err != nil {
			t.Fatalf("Dial %d: %v", i, err)
		}
		connsA = append(connsA, cA)

		_, err = ln.Accept()
		if err != nil {
			t.Fatalf("Accept %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	closeDone := make(chan struct{})

	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			connsA[idx].Write([]byte("data"))
		}(i)
	}

	go func() {
		defer close(closeDone)
		time.Sleep(5 * time.Millisecond)
		connsA[0].Close()
	}()

	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung — control was blocked by data")
	}

	wg.Wait()

	for i := 1; i < numConns; i++ {
		connsA[i].Close()
	}
}
