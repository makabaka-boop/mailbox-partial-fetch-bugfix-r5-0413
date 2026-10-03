package imapd

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// ---- test infrastructure -------------------------------------------------

func startTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	store, err := OpenStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	srv := NewServer(store)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(ln)
	t.Cleanup(func() {
		srv.Close()
		store.Close()
	})
	return srv, ln.Addr().String()
}

// testClient is a real TCP client speaking the protocol over a socket.
type testClient struct {
	t    *testing.T
	name string
	conn net.Conn
	br   *bufio.Reader
	tagN int
}

func newTestClient(t *testing.T, name, addr string) *testClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("%s: dial: %v", name, err)
	}
	c := &testClient{t: t, name: name, conn: conn, br: bufio.NewReader(conn)}
	c.expectLines("* OK imapd ready")
	return c
}

func (c *testClient) nextTag() string {
	c.tagN++
	return fmt.Sprintf("t%d", c.tagN)
}

// send writes s completely (TCP Write may accept only part of a large buffer).
func (c *testClient) send(s string) {
	c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	data := []byte(s)
	for len(data) > 0 {
		n, err := c.conn.Write(data)
		if err != nil {
			c.t.Fatalf("%s: write: %v", c.name, err)
		}
		data = data[n:]
	}
}

func (c *testClient) readLine() string {
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := c.br.ReadString('\n')
	if err != nil {
		c.t.Fatalf("%s: read line: %v", c.name, err)
	}
	line = strings.TrimSuffix(line, "\n")
	line = strings.TrimSuffix(line, "\r")
	return line
}

// expectLines fails unless the next lines match want exactly, in order.
func (c *testClient) expectLines(want ...string) {
	c.t.Helper()
	for _, w := range want {
		if got := c.readLine(); got != w {
			c.t.Fatalf("%s: got  %q\nwant %q", c.name, got, w)
		}
	}
}

func (c *testClient) command(format string, args ...interface{}) string {
	tag := c.nextTag()
	c.send(tag + " " + fmt.Sprintf(format, args...) + "\r\n")
	return tag
}

// appendMsg sends a complete APPEND command (command line + literal + CRLF).
func (c *testClient) appendMsg(box, flags, body string) string {
	tag := c.nextTag()
	var sb strings.Builder
	if flags != "" {
		fmt.Fprintf(&sb, "%s APPEND %s %s {%d}\r\n", tag, box, flags, len(body))
	} else {
		fmt.Fprintf(&sb, "%s APPEND %s {%d}\r\n", tag, box, len(body))
	}
	sb.WriteString(body)
	sb.WriteString("\r\n")
	c.send(sb.String())
	return tag
}

func appendOK(tag string, uid int) string {
	return fmt.Sprintf("%s OK [APPENDUID 1 %d] APPEND completed", tag, uid)
}

func exists(n int) string { return fmt.Sprintf("* %d EXISTS", n) }

// expectFetchBody reads a "* seq FETCH (... BODY[] {n}" header, then exactly
// n literal bytes, then the closing ")".
func (c *testClient) expectFetchBody(wantHeaderPrefix, wantBody string) {
	c.t.Helper()
	header := c.readLine()
	if !strings.HasPrefix(header, wantHeaderPrefix) || !strings.HasSuffix(header, "}") {
		c.t.Fatalf("%s: bad FETCH header %q (want prefix %q)", c.name, header, wantHeaderPrefix)
	}
	open := strings.LastIndexByte(header, '{')
	n, err := strconv.Atoi(header[open+1 : len(header)-1])
	if err != nil {
		c.t.Fatalf("%s: bad literal size in %q", c.name, header)
	}
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	body := make([]byte, n)
	if _, err := io.ReadFull(c.br, body); err != nil {
		c.t.Fatalf("%s: read literal: %v", c.name, err)
	}
	if string(body) != wantBody {
		c.t.Fatalf("%s: body mismatch:\ngot  %q\nwant %q", c.name, body, wantBody)
	}
	c.expectLines(")")
}

func (c *testClient) close() { c.conn.Close() }

// ---- the main scenario ----------------------------------------------------

// TestTwoClientsInterleaved drives two real TCP clients (A and B) selected on
// the same mailbox through interleaved appends, flag changes and expunges,
// verifying on both connections that change notifications arrive in mailbox
// revision order and that UID-based queries see stable message identities.
func TestTwoClientsInterleaved(t *testing.T) {
	_, addr := startTestServer(t)

	a := newTestClient(t, "A", addr)
	defer a.close()
	b := newTestClient(t, "B", addr)
	defer b.close()

	// Both clients select the same (initially empty) mailbox.
	tag := a.command("SELECT INBOX")
	a.expectLines("* 0 EXISTS", "* OK [UIDNEXT 1] next uid", tag+" OK SELECT completed")
	tag = b.command("SELECT INBOX")
	b.expectLines("* 0 EXISTS", "* OK [UIDNEXT 1] next uid", tag+" OK SELECT completed")

	// Bodies contain newlines, command-looking text and literal-looking text;
	// all of it must be treated as payload thanks to the byte-counted literal.
	m1 := "From: alice@example.com\r\nSubject: first\r\n\r\nhello\r\nz NOOP\r\na SELECT INBOX\r\n"
	m2 := "second {3} literal-looking\r\nbody with CRLF\r\n"
	m3 := "t9 UID FETCH 1:* (FLAGS)\r\nnot really a command"

	// A sends three APPENDs pipelined in a single TCP write.
	t1 := a.nextTag()
	t2 := a.nextTag()
	t3 := a.nextTag()
	a.send(fmt.Sprintf("%s APPEND INBOX {%d}\r\n%s\r\n", t1, len(m1), m1) +
		fmt.Sprintf("%s APPEND INBOX (\\Seen) {%d}\r\n%s\r\n", t2, len(m2), m2) +
		fmt.Sprintf("%s APPEND INBOX {%d}\r\n%s\r\n", t3, len(m3), m3))
	a.expectLines(
		exists(1), appendOK(t1, 1),
		exists(2), appendOK(t2, 2),
		exists(3), appendOK(t3, 3),
	)
	// B observes the three revisions in order.
	b.expectLines(exists(1), exists(2), exists(3))

	// B appends one message, dribbled out in 5-byte TCP chunks: commands may
	// arrive split across any number of segments.
	m4 := "fourth message from B"
	tb2 := b.nextTag()
	wire := fmt.Sprintf("%s APPEND INBOX {%d}\r\n%s\r\n", tb2, len(m4), m4)
	for i := 0; i < len(wire); i += 5 {
		end := i + 5
		if end > len(wire) {
			end = len(wire)
		}
		b.send(wire[i:end])
	}
	b.expectLines(exists(4), appendOK(tb2, 4))
	a.expectLines(exists(4))

	// A marks UID 2 deleted; both sessions get the same untagged update.
	tag = a.command(`UID STORE 2 +FLAGS (\Deleted)`)
	a.expectLines(`* 2 FETCH (UID 2 FLAGS (\Deleted \Seen))`, tag+" OK STORE completed")
	b.expectLines(`* 2 FETCH (UID 2 FLAGS (\Deleted \Seen))`)

	// B marks UIDs 1 and 3 seen.
	tag = b.command(`UID STORE 1,3 +FLAGS (\Seen)`)
	b.expectLines(
		`* 1 FETCH (UID 1 FLAGS (\Seen))`,
		`* 3 FETCH (UID 3 FLAGS (\Seen))`,
		tag+" OK STORE completed",
	)
	a.expectLines(
		`* 1 FETCH (UID 1 FLAGS (\Seen))`,
		`* 3 FETCH (UID 3 FLAGS (\Seen))`,
	)

	// A expunges. UID 2 disappears; sequence numbers above it renumber.
	tag = a.command("EXPUNGE")
	a.expectLines("* 2 EXPUNGE", tag+" OK EXPUNGE completed")
	b.expectLines("* 2 EXPUNGE")
	// State: uid1(\Seen)=seq1, uid3()=seq2, uid4()=seq3.

	// Identity check: UID 2 is gone forever. The message now sitting at
	// sequence 2 (UID 3) must not be addressable through the old identity.
	tag = a.command("UID FETCH 2 (FLAGS)")
	a.expectLines(tag + " OK FETCH completed") // no untagged data
	tag = a.command(`UID STORE 2 +FLAGS (\Deleted)`)
	a.expectLines(tag + " OK STORE completed") // no-op, no untagged data
	tag = a.command("UID FETCH 3 (FLAGS)")
	a.expectLines(`* 2 FETCH (UID 3 FLAGS (\Seen))`, tag+" OK FETCH completed")

	// The body of UID 1 — including its fake command lines — survives intact.
	tag = b.command("UID FETCH 1 (BODY[])")
	b.expectFetchBody(fmt.Sprintf("* 1 FETCH (UID 1 FLAGS (\\Seen) BODY[] {%d}", len(m1)), m1)
	b.expectLines(tag + " OK FETCH completed")

	// Flag replace, flag removal, and .SILENT (originator sees no untagged
	// response, the other session still does).
	tag = a.command(`UID STORE 4 FLAGS (\Answered)`)
	a.expectLines(`* 3 FETCH (UID 4 FLAGS (\Answered))`, tag+" OK STORE completed")
	b.expectLines(`* 3 FETCH (UID 4 FLAGS (\Answered))`)
	tag = a.command(`UID STORE 4 -FLAGS (\Answered)`)
	a.expectLines(`* 3 FETCH (UID 4 FLAGS ())`, tag+" OK STORE completed")
	b.expectLines(`* 3 FETCH (UID 4 FLAGS ())`)
	tag = b.command(`UID STORE 4 +FLAGS.SILENT (\Draft)`)
	b.expectLines(tag + " OK STORE completed")
	a.expectLines(`* 3 FETCH (UID 4 FLAGS (\Draft))`)

	// Interleaved appends: A and B alternate, five each. Every client must
	// see the EXISTS notifications in mailbox revision order (counts 4..13).
	for i := 0; i < 5; i++ {
		uid := 5 + 2*i
		n := 4 + 2*i
		tag = a.appendMsg("INBOX", "", fmt.Sprintf("bulk A %d", i))
		a.expectLines(exists(n), appendOK(tag, uid))
		b.expectLines(exists(n))

		uid = 6 + 2*i
		n = 5 + 2*i
		tag = b.appendMsg("INBOX", "", fmt.Sprintf("bulk B %d", i))
		b.expectLines(exists(n), appendOK(tag, uid))
		a.expectLines(exists(n))
	}
	// State: 13 messages, uids [1,3,4,5..14].

	// Full UID query: UIDs are stable identities, sequence numbers reflect
	// the current (renumbered) order.
	tag = a.command("UID FETCH 1:* (FLAGS)")
	a.expectLines(
		`* 1 FETCH (UID 1 FLAGS (\Seen))`,
		`* 2 FETCH (UID 3 FLAGS (\Seen))`,
		`* 3 FETCH (UID 4 FLAGS (\Draft))`,
		`* 4 FETCH (UID 5 FLAGS ())`,
		`* 5 FETCH (UID 6 FLAGS ())`,
		`* 6 FETCH (UID 7 FLAGS ())`,
		`* 7 FETCH (UID 8 FLAGS ())`,
		`* 8 FETCH (UID 9 FLAGS ())`,
		`* 9 FETCH (UID 10 FLAGS ())`,
		`* 10 FETCH (UID 11 FLAGS ())`,
		`* 11 FETCH (UID 12 FLAGS ())`,
		`* 12 FETCH (UID 13 FLAGS ())`,
		`* 13 FETCH (UID 14 FLAGS ())`,
		tag+" OK FETCH completed",
	)

	// UID ranges with "*" resolve against the current mailbox.
	tag = a.command("UID FETCH 8:* (FLAGS)")
	a.expectLines(
		`* 7 FETCH (UID 8 FLAGS ())`,
		`* 8 FETCH (UID 9 FLAGS ())`,
		`* 9 FETCH (UID 10 FLAGS ())`,
		`* 10 FETCH (UID 11 FLAGS ())`,
		`* 11 FETCH (UID 12 FLAGS ())`,
		`* 12 FETCH (UID 13 FLAGS ())`,
		`* 13 FETCH (UID 14 FLAGS ())`,
		tag+" OK FETCH completed",
	)

	// Delete two messages in one EXPUNGE: the per-message EXPUNGE events must
	// carry the sequence numbers valid at each deletion step (4, then 5).
	tag = a.command(`UID STORE 5,7 +FLAGS (\Deleted)`)
	a.expectLines(
		`* 4 FETCH (UID 5 FLAGS (\Deleted))`,
		`* 6 FETCH (UID 7 FLAGS (\Deleted))`,
		tag+" OK STORE completed",
	)
	b.expectLines(
		`* 4 FETCH (UID 5 FLAGS (\Deleted))`,
		`* 6 FETCH (UID 7 FLAGS (\Deleted))`,
	)
	tag = a.command("EXPUNGE")
	a.expectLines("* 4 EXPUNGE", "* 5 EXPUNGE", tag+" OK EXPUNGE completed")
	b.expectLines("* 4 EXPUNGE", "* 5 EXPUNGE")
	// State: 11 messages, uids [1,3,4,6,8,9,10,11,12,13,14].

	// A client that disconnects in the middle of an APPEND literal must not
	// leave half a message behind.
	c := newTestClient(t, "C", addr)
	tag = c.command("SELECT INBOX")
	c.expectLines("* 11 EXISTS", "* OK [UIDNEXT 15] next uid", tag+" OK SELECT completed")
	c.send(fmt.Sprintf("%s APPEND INBOX {100}\r\n", c.nextTag()))
	c.send("partial-body") // 12 of the promised 100 bytes, then vanish
	c.close()
	time.Sleep(250 * time.Millisecond)

	tag = a.command("UID FETCH 1:* (FLAGS)")
	a.expectLines(
		`* 1 FETCH (UID 1 FLAGS (\Seen))`,
		`* 2 FETCH (UID 3 FLAGS (\Seen))`,
		`* 3 FETCH (UID 4 FLAGS (\Draft))`,
		`* 4 FETCH (UID 6 FLAGS ())`,
		`* 5 FETCH (UID 8 FLAGS ())`,
		`* 6 FETCH (UID 9 FLAGS ())`,
		`* 7 FETCH (UID 10 FLAGS ())`,
		`* 8 FETCH (UID 11 FLAGS ())`,
		`* 9 FETCH (UID 12 FLAGS ())`,
		`* 10 FETCH (UID 13 FLAGS ())`,
		`* 11 FETCH (UID 14 FLAGS ())`,
		tag+" OK FETCH completed",
	)

	// The aborted APPEND consumed no UID: the next one is 15.
	tag = a.appendMsg("INBOX", "", "post-disconnect")
	a.expectLines(exists(12), appendOK(tag, 15))
	b.expectLines(exists(12))

	// Per-message size limit: 16 KB + 1 byte is rejected, the oversized
	// literal is consumed, and the connection stays usable.
	oversize := strings.Repeat("x", MaxMessageSize+1)
	tag = a.nextTag()
	a.send(fmt.Sprintf("%s APPEND INBOX {%d}\r\n%s\r\n", tag, len(oversize), oversize))
	a.expectLines(tag + " NO message exceeds the 16 KB limit")
	tag = a.command("NOOP")
	a.expectLines(tag + " OK NOOP completed")

	// Fill the mailbox to its 200-message limit, pipelining the commands in
	// one burst (the server must handle back-to-back commands), then reading
	// every reply in order.
	fillTags := make([]string, 0, 188)
	var fill strings.Builder
	for i := 1; i <= 188; i++ {
		ftag := a.nextTag()
		fillTags = append(fillTags, ftag)
		body := fmt.Sprintf("fill %d", i)
		fmt.Fprintf(&fill, "%s APPEND INBOX {%d}\r\n%s\r\n", ftag, len(body), body)
	}
	a.send(fill.String())
	for i, ftag := range fillTags {
		a.expectLines(exists(13+i), appendOK(ftag, 16+i))
	}
	// B received every revision in order.
	for n := 13; n <= 200; n++ {
		b.expectLines(exists(n))
	}

	// Message 201 is refused.
	tag = a.appendMsg("INBOX", "", "one too many")
	a.expectLines(tag + " NO mailbox full (200 messages)")

	// A fresh connection sees the final state; UIDs were never reused
	// (203 assigned, 3 expunged => 200 present, next UID 204).
	d := newTestClient(t, "D", addr)
	defer d.close()
	tag = d.command("SELECT INBOX")
	d.expectLines("* 200 EXISTS", "* OK [UIDNEXT 204] next uid", tag+" OK SELECT completed")
	tag = d.command("LOGOUT")
	d.expectLines("* BYE imapd logging out", tag+" OK LOGOUT completed")
}

// ---- parser unit tests ----------------------------------------------------

func TestUIDSetParsing(t *testing.T) {
	set, err := parseUIDSet("1,3,5:7,*", 10)
	if err != nil {
		t.Fatalf("parseUIDSet: %v", err)
	}
	want := []uidInterval{{1, 1}, {3, 3}, {5, 7}, {10, 10}}
	if !reflect.DeepEqual(set, want) {
		t.Fatalf("got %v want %v", set, want)
	}
	for _, uid := range []uint64{1, 3, 5, 6, 7, 10} {
		if !uidInSet(set, uid) {
			t.Fatalf("uid %d should be in set", uid)
		}
	}
	for _, uid := range []uint64{2, 4, 8, 9, 11} {
		if uidInSet(set, uid) {
			t.Fatalf("uid %d should not be in set", uid)
		}
	}
	for _, bad := range []string{"", "0", "a", "1:", ":2", "1,,2"} {
		if _, err := parseUIDSet(bad, 10); err == nil {
			t.Fatalf("parseUIDSet(%q) should fail", bad)
		}
	}
	// Reversed ranges are normalized (IMAP semantics).
	set, err = parseUIDSet("7:5", 10)
	if err != nil || !reflect.DeepEqual(set, []uidInterval{{5, 7}}) {
		t.Fatalf("got %v, %v", set, err)
	}
}

func TestFlagOps(t *testing.T) {
	if got := applyFlagOp(nil, []string{`\Seen`}, 1); !reflect.DeepEqual(got, []string{`\Seen`}) {
		t.Fatalf("add: %v", got)
	}
	if got := applyFlagOp([]string{`\Seen`}, []string{`\Deleted`}, 1); !reflect.DeepEqual(got, []string{`\Deleted`, `\Seen`}) {
		t.Fatalf("add merge: %v", got)
	}
	if got := applyFlagOp([]string{`\Deleted`, `\Seen`}, []string{`\Seen`}, 2); !reflect.DeepEqual(got, []string{`\Deleted`}) {
		t.Fatalf("remove: %v", got)
	}
	if got := applyFlagOp([]string{`\Seen`}, []string{`\Answered`}, 0); !reflect.DeepEqual(got, []string{`\Answered`}) {
		t.Fatalf("replace: %v", got)
	}
}
