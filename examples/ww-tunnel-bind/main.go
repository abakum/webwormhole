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
