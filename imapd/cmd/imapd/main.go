// Command imapd runs the limited-IMAP TCP server.
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"imapd"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:1143", "TCP listen address")
	dbPath := flag.String("db", "imapd.db", "SQLite database file")
	flag.Parse()

	store, err := imapd.OpenStore(*dbPath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer store.Close()

	srv := imapd.NewServer(store)
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("imapd listening on %s, database %s", ln.Addr(), *dbPath)
	go srv.Serve(ln)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Println("shutting down")
	srv.Close()
}
