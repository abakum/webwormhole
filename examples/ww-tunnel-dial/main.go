package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"webwormhole.io/wormhole"
)

func main() {
	code := flag.String("code", "", "wormhole code (leave empty to generate)")
	dial := flag.String("dial", ":8080", "local address to serve and expose via tunnel")
	sigserv := flag.String("signal", wormhole.DefaultSignalServer, "signalling server")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "hello from webwormhole tunnel\n")
	})
	srv := &http.Server{Addr: *dial}
	go srv.ListenAndServe()
	go func() {
		<-ctx.Done()
		srv.Close()
	}()

	c := wormhole.Client{SignalServer: *sigserv}

	generatedCode, connect, err := c.PrepareTunnel(ctx, *code)
	if err != nil {
		log.Fatalf("prepare tunnel: %v", err)
	}
	log.Printf("wormhole code: %s", generatedCode)

	t, err := connect()
	if err != nil {
		log.Fatalf("connect tunnel: %v", err)
	}
	defer t.Close()
	log.Printf("tunnel created, serving %s", *dial)
	if err := t.Serve(ctx, *dial); err != nil && ctx.Err() == nil {
		log.Fatalf("dial: %v", err)
	}
}
