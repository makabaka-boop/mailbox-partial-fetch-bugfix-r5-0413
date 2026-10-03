package imapd

import (
	"net"
	"sync"
)

const (
	// MaxMessageSize is the per-message body limit (16 KB).
	MaxMessageSize = 16 * 1024
	// MaxMessages is the per-mailbox message limit.
	MaxMessages = 200

	maxLineLength = 8192
	// maxLiteral is the largest APPEND literal the server will read before
	// rejecting; oversized literals are consumed and discarded so the
	// connection stays in sync, but absurd sizes close the connection.
	maxLiteral = 1 << 20
	// outBufferSize bounds each session's outbound queue. It must comfortably
	// hold the notifications a client can be owed (one per mailbox revision).
	outBufferSize = 4096
)

// Server accepts IMAP-like client connections and coordinates per-mailbox
// notification ordering.
type Server struct {
	store *Store

	mu       sync.Mutex
	hubs     map[string]*mailboxHub
	sessions map[*Session]struct{}
	ln       net.Listener

	wg sync.WaitGroup
}

// NewServer creates a Server backed by store.
func NewServer(store *Store) *Server {
	return &Server{
		store:    store,
		hubs:     make(map[string]*mailboxHub),
		sessions: make(map[*Session]struct{}),
	}
}

// Serve accepts connections until the listener is closed.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		sess := newSession(s, conn)
		s.mu.Lock()
		s.sessions[sess] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go sess.run()
	}
}

// Addr returns the listener address, or nil before Serve is called.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Close stops the listener and all client connections, then waits for the
// session goroutines to finish.
func (s *Server) Close() {
	s.mu.Lock()
	if s.ln != nil {
		s.ln.Close()
	}
	sessions := make([]*Session, 0, len(s.sessions))
	for sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	s.mu.Unlock()
	// Closing the connection makes the session's read loop fail; the session
	// then cleans itself up on its own goroutine (no shared-state races).
	for _, sess := range sessions {
		sess.conn.Close()
	}
	s.wg.Wait()
}

// hub returns the mailboxHub for name, creating it on first use.
func (s *Server) hub(name string) *mailboxHub {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.hubs[name]
	if !ok {
		h = &mailboxHub{sessions: make(map[*Session]struct{})}
		s.hubs[name] = h
	}
	return h
}

func (s *Server) removeSession(sess *Session) {
	s.mu.Lock()
	delete(s.sessions, sess)
	s.mu.Unlock()
}

// mailboxHub serializes all mutations of one mailbox. Because every change is
// applied to SQLite and broadcast while holding mu, the broadcasts constitute
// the mailbox's revision order, and every selected session receives them in
// that exact order through its FIFO outbound queue.
type mailboxHub struct {
	mu       sync.Mutex
	sessions map[*Session]struct{}
}

func (h *mailboxHub) add(s *Session) {
	h.mu.Lock()
	h.sessions[s] = struct{}{}
	h.mu.Unlock()
}

func (h *mailboxHub) remove(s *Session) {
	h.mu.Lock()
	delete(h.sessions, s)
	h.mu.Unlock()
}

// broadcast enqueues frames on every selected session. If skipOrigin is set,
// origin is excluded (used for STORE .SILENT). Callers must hold h.mu.
func (h *mailboxHub) broadcast(origin *Session, skipOrigin bool, frames ...[]byte) {
	for sess := range h.sessions {
		if skipOrigin && sess == origin {
			continue
		}
		for _, f := range frames {
			select {
			case sess.out <- f:
			case <-sess.done:
			}
		}
	}
}
