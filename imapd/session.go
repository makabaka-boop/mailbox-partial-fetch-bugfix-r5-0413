package imapd

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

// Session is one client connection. All responses — both command replies and
// asynchronous mailbox notifications — flow through the single FIFO out
// channel, so a client always observes mailbox revisions in order.
type Session struct {
	srv  *Server
	conn net.Conn
	br   *bufio.Reader
	out  chan []byte
	done chan struct{}

	closeOnce sync.Once
	writerWG  sync.WaitGroup

	// smu guards the selected-mailbox fields below.
	smu sync.Mutex
	box string
	hub *mailboxHub
}

func newSession(srv *Server, conn net.Conn) *Session {
	return &Session{
		srv:  srv,
		conn: conn,
		br:   bufio.NewReader(conn),
		out:  make(chan []byte, outBufferSize),
		done: make(chan struct{}),
	}
}

func (s *Session) run() {
	defer s.srv.wg.Done()
	// Shutdown order matters: cleanup() closes the out queue, the writer then
	// flushes every queued frame (including the LOGOUT reply) to the
	// connection, and only then is the connection closed.
	defer s.conn.Close()
	defer s.writerWG.Wait()
	defer s.cleanup()

	s.writerWG.Add(1)
	go s.writer()

	s.sendf("* OK imapd ready")
	for {
		line, err := readLine(s.br)
		if err != nil {
			return // includes clients vanishing mid-command
		}
		if line == "" {
			continue
		}
		if !s.dispatch(line) {
			return
		}
	}
}

// writer is the only goroutine that writes to the connection.
func (s *Session) writer() {
	defer s.writerWG.Done()
	for b := range s.out {
		s.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if _, err := s.conn.Write(b); err != nil {
			return
		}
	}
}

func (s *Session) cleanup() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.smu.Lock()
		h := s.hub
		s.hub, s.box = nil, ""
		s.smu.Unlock()
		if h != nil {
			h.remove(s)
		}
		close(s.out)
		s.srv.removeSession(s)
	})
}

// enqueue offers a frame to the outbound queue; it never blocks a dead session.
func (s *Session) enqueue(b []byte) {
	select {
	case s.out <- b:
	case <-s.done:
	}
}

func (s *Session) sendf(format string, args ...interface{}) {
	s.enqueue([]byte(fmt.Sprintf(format, args...) + "\r\n"))
}

func (s *Session) selected() (*mailboxHub, string) {
	s.smu.Lock()
	defer s.smu.Unlock()
	return s.hub, s.box
}

// dispatch executes one command line. It returns false when the connection
// should be closed.
func (s *Session) dispatch(line string) bool {
	tag, rest, ok := cutSpace(line)
	if !ok || tag == "" {
		s.sendf("* BAD invalid command line")
		return true
	}
	cmd, args, _ := cutSpace(rest)
	switch strings.ToUpper(cmd) {
	case "SELECT":
		s.cmdSelect(tag, args)
	case "APPEND":
		return s.cmdAppend(tag, args)
	case "UID":
		s.cmdUID(tag, args)
	case "EXPUNGE":
		s.cmdExpunge(tag)
	case "NOOP":
		s.sendf("%s OK NOOP completed", tag)
	case "LOGOUT":
		s.sendf("* BYE imapd logging out")
		s.sendf("%s OK LOGOUT completed", tag)
		return false
	default:
		s.sendf("%s BAD unknown command", tag)
	}
	return true
}

func (s *Session) cmdSelect(tag, args string) {
	box := strings.TrimSpace(args)
	if box == "" || strings.ContainsAny(box, " \t") {
		s.sendf("%s BAD invalid mailbox name", tag)
		return
	}
	// Leave any previously selected mailbox first (single lock at a time, so
	// two sessions switching mailboxes can never deadlock).
	s.smu.Lock()
	old := s.hub
	s.hub, s.box = nil, ""
	s.smu.Unlock()
	if old != nil {
		old.remove(s)
	}

	h := s.srv.hub(box)
	h.mu.Lock()
	count, nextUID, err := s.srv.store.MailboxInfo(box)
	if err != nil {
		h.mu.Unlock()
		s.sendf("%s NO storage error", tag)
		return
	}
	// Register atomically with the snapshot: notifications for revisions
	// after this point are guaranteed to reach us in order.
	h.sessions[s] = struct{}{}
	s.smu.Lock()
	s.hub, s.box = h, box
	s.smu.Unlock()
	h.mu.Unlock()

	s.sendf("* %d EXISTS", count)
	s.sendf("* OK [UIDNEXT %d] next uid", nextUID)
	s.sendf("%s OK SELECT completed", tag)
}

func (s *Session) cmdAppend(tag, args string) bool {
	box, flags, size, err := parseAppendArgs(args)
	if err != nil {
		s.sendf("%s BAD invalid APPEND arguments", tag)
		return true
	}
	if size > maxLiteral {
		// Declared size is absurd; we cannot resynchronize the stream.
		s.sendf("%s BAD literal too large", tag)
		return false
	}
	// Read exactly the declared number of bytes, however they arrive. If the
	// client disconnects here, ReadFull fails and nothing is stored — a
	// half-written message is impossible.
	body := make([]byte, size)
	if _, err := io.ReadFull(s.br, body); err != nil {
		return false
	}
	var crlf [2]byte
	if _, err := io.ReadFull(s.br, crlf[:]); err != nil {
		return false
	}
	if crlf[0] != '\r' || crlf[1] != '\n' {
		s.sendf("%s BAD expected CRLF after literal", tag)
		return false
	}
	if size > MaxMessageSize {
		// The literal was consumed and discarded above, so the connection
		// stays in sync and the client may continue issuing commands.
		s.sendf("%s NO message exceeds the 16 KB limit", tag)
		return true
	}

	h := s.srv.hub(box)
	h.mu.Lock()
	uid, count, err := s.srv.store.Append(box, flags, body)
	if err == ErrMailboxFull {
		h.mu.Unlock()
		s.sendf("%s NO mailbox full (%d messages)", tag, MaxMessages)
		return true
	}
	if err != nil {
		h.mu.Unlock()
		s.sendf("%s NO storage error", tag)
		return true
	}
	// New revision: tell every selected session, including the originator.
	h.broadcast(nil, false, []byte(fmt.Sprintf("* %d EXISTS\r\n", count)))
	h.mu.Unlock()

	s.sendf("%s OK [APPENDUID 1 %d] APPEND completed", tag, uid)
	return true
}

func (s *Session) cmdUID(tag, args string) {
	sub, rest, ok := cutSpace(args)
	if !ok {
		s.sendf("%s BAD UID requires a subcommand", tag)
		return
	}
	switch strings.ToUpper(sub) {
	case "FETCH":
		s.cmdUIDFetch(tag, rest)
	case "STORE":
		s.cmdUIDStore(tag, rest)
	default:
		s.sendf("%s BAD unknown UID subcommand", tag)
	}
}

func (s *Session) cmdUIDFetch(tag, args string) {
	s.cmdDetailedFetch(tag, args)
}

func (s *Session) cmdUIDFetchLegacy(tag, args string) {
	h, box := s.selected()
	if h == nil {
		s.sendf("%s BAD no mailbox selected", tag)
		return
	}
	setSpec, items, ok := cutSpace(args)
	if !ok || items == "" {
		s.sendf("%s BAD invalid FETCH arguments", tag)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	msgs, err := s.srv.store.List(box, true)
	if err != nil {
		s.sendf("%s NO storage error", tag)
		return
	}
	set, err := parseUIDSet(setSpec, maxUID(msgs))
	if err != nil {
		s.sendf("%s BAD invalid UID set", tag)
		return
	}
	wantBody := strings.Contains(strings.ToUpper(items), "BODY")
	// Sequence numbers are computed here, under the hub lock, from the
	// current UID ordering — never cached, so a renumbering caused by an
	// EXPUNGE can never be confused with a message's identity.
	for i, m := range msgs {
		if !uidInSet(set, m.UID) {
			continue
		}
		seq := i + 1
		if wantBody {
			var b bytes.Buffer
			fmt.Fprintf(&b, "* %d FETCH (UID %d FLAGS (%s) BODY[] {%d}\r\n",
				seq, m.UID, joinFlags(m.Flags), len(m.Body))
			b.Write(m.Body)
			b.WriteString(")\r\n")
			s.enqueue(b.Bytes())
		} else {
			s.sendf("* %d FETCH (UID %d FLAGS (%s))", seq, m.UID, joinFlags(m.Flags))
		}
	}
	s.sendf("%s OK FETCH completed", tag)
}

func (s *Session) cmdUIDStore(tag, args string) {
	h, box := s.selected()
	if h == nil {
		s.sendf("%s BAD no mailbox selected", tag)
		return
	}
	setSpec, rest, ok := cutSpace(args)
	if !ok {
		s.sendf("%s BAD invalid STORE arguments", tag)
		return
	}
	op, flagSpec, ok := cutSpace(rest)
	if !ok || flagSpec == "" {
		s.sendf("%s BAD invalid STORE arguments", tag)
		return
	}
	var mode int // 0 replace, 1 add, 2 remove
	silent := false
	switch strings.ToUpper(op) {
	case "FLAGS":
		mode = 0
	case "FLAGS.SILENT":
		mode, silent = 0, true
	case "+FLAGS":
		mode = 1
	case "+FLAGS.SILENT":
		mode, silent = 1, true
	case "-FLAGS":
		mode = 2
	case "-FLAGS.SILENT":
		mode, silent = 2, true
	default:
		s.sendf("%s BAD unknown STORE operation", tag)
		return
	}
	flags, err := parseFlagList(flagSpec)
	if err != nil {
		s.sendf("%s BAD invalid flag list", tag)
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	msgs, err := s.srv.store.List(box, false)
	if err != nil {
		s.sendf("%s NO storage error", tag)
		return
	}
	set, err := parseUIDSet(setSpec, maxUID(msgs))
	if err != nil {
		s.sendf("%s BAD invalid UID set", tag)
		return
	}
	var frames [][]byte
	for i, m := range msgs {
		if !uidInSet(set, m.UID) {
			continue
		}
		newFlags := applyFlagOp(m.Flags, flags, mode)
		if err := s.srv.store.SetFlags(box, m.UID, newFlags); err != nil {
			s.sendf("%s NO storage error", tag)
			return
		}
		frames = append(frames, []byte(fmt.Sprintf("* %d FETCH (UID %d FLAGS (%s))\r\n",
			i+1, m.UID, joinFlags(newFlags))))
	}
	// One revision for the whole STORE; .SILENT suppresses the untagged
	// responses only for the originator.
	h.broadcast(s, silent, frames...)
	s.sendf("%s OK STORE completed", tag)
}

func (s *Session) cmdExpunge(tag string) {
	h, box := s.selected()
	if h == nil {
		s.sendf("%s BAD no mailbox selected", tag)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	msgs, err := s.srv.store.List(box, false)
	if err != nil {
		s.sendf("%s NO storage error", tag)
		return
	}
	// Emit one "* n EXPUNGE" per deleted message, where n is the sequence
	// number the message has at the moment of its deletion (i.e. after the
	// previously reported deletions). Every selected session receives these
	// events in the same order, so all of them can renumber consistently.
	remaining := make([]uint64, len(msgs))
	for i, m := range msgs {
		remaining[i] = m.UID
	}
	var frames [][]byte
	var doomed []uint64
	for _, m := range msgs {
		if !hasFlag(m.Flags, `\Deleted`) {
			continue
		}
		for j, uid := range remaining {
			if uid == m.UID {
				frames = append(frames, []byte(fmt.Sprintf("* %d EXPUNGE\r\n", j+1)))
				remaining = append(remaining[:j:j], remaining[j+1:]...)
				break
			}
		}
		doomed = append(doomed, m.UID)
	}
	if err := s.srv.store.Delete(box, doomed); err != nil {
		s.sendf("%s NO storage error", tag)
		return
	}
	h.broadcast(nil, false, frames...)
	s.sendf("%s OK EXPUNGE completed", tag)
}

func maxUID(msgs []Message) uint64 {
	if len(msgs) == 0 {
		return 0
	}
	return msgs[len(msgs)-1].UID
}
