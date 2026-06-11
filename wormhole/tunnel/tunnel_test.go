package tunnel

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestTunnelServeForward(t *testing.T) {
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for http: %v", err)
	}
	httpSrv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "hello from tunnel, path=%s", r.URL.Path)
		}),
	}
	go httpSrv.Serve(httpLis)
	defer httpSrv.Close()

	httpAddr := httpLis.Addr().String()

	aIO, bIO := newPipeRecordIOPair()

	sessA := NewSession(aIO)
	sessB := NewSession(bIO)
	tunnelA := NewTunnel(sessA)
	tunnelB := NewTunnel(sessB)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go tunnelA.Serve(ctx, httpAddr)

	bindLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for bind: %v", err)
	}
	bindAddr := bindLis.Addr().String()
	bindLis.Close()

	go tunnelB.Forward(ctx, bindAddr)

	time.Sleep(50 * time.Millisecond)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://%s/testpath", bindAddr))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	expected := "hello from tunnel, path=/testpath"
	if string(body) != expected {
		t.Fatalf("expected %q, got %q", expected, string(body))
	}

	tunnelA.Close()
	tunnelB.Close()
}

func TestTunnelMultipleRequests(t *testing.T) {
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for http: %v", err)
	}
	httpSrv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "response-%s", r.URL.Path[1:])
		}),
	}
	go httpSrv.Serve(httpLis)
	defer httpSrv.Close()

	httpAddr := httpLis.Addr().String()

	aIO, bIO := newPipeRecordIOPair()

	sessA := NewSession(aIO)
	sessB := NewSession(bIO)
	tunnelA := NewTunnel(sessA)
	tunnelB := NewTunnel(sessB)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go tunnelA.Serve(ctx, httpAddr)

	bindLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for bind: %v", err)
	}
	bindAddr := bindLis.Addr().String()
	bindLis.Close()

	go tunnelB.Forward(ctx, bindAddr)

	time.Sleep(50 * time.Millisecond)

	client := &http.Client{Timeout: 5 * time.Second}
	for i := 0; i < 3; i++ {
		path := fmt.Sprintf("req%d", i)
		resp, err := client.Get(fmt.Sprintf("http://%s/%s", bindAddr, path))
		if err != nil {
			t.Fatalf("GET %d: %v", i, err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("read body %d: %v", i, err)
		}
		expected := "response-" + path
		if string(body) != expected {
			t.Fatalf("req %d: expected %q, got %q", i, expected, string(body))
		}
	}

	tunnelA.Close()
	tunnelB.Close()
}
