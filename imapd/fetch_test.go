package imapd

import (
	"io"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// ---- frame parsing for multi-literal FETCH responses ----------------------

// fetchBody is one BODY section found inside a FETCH frame.
type fetchBody struct {
	section string // e.g. "BODY[]" or "BODY[]<3.5>"
	data    string
}

// fetchFrame is the parsed content of one untagged FETCH response.
type fetchFrame struct {
	seq    int
	uid    int
	flags  string
	bodies []fetchBody
}

// readFetchFrames sends nothing; it reads the untagged FETCH frames of a
// command already sent and stops at its tagged completion line. A frame may
// carry any number of {n} literals, with subsequent section headers arriving
// on the lines after each literal.
func (c *testClient) readFetchFrames(tag string) []fetchFrame {
	c.t.Helper()
	var out []fetchFrame
	for {
		line := c.readLine()
		if strings.HasPrefix(line, tag+" ") {
			if !strings.HasPrefix(line, tag+" OK") {
				c.t.Fatalf("%s: command not OK: %q", c.name, line)
			}
			return out
		}
		open := strings.Index(line, " FETCH (")
		if !strings.HasPrefix(line, "* ") || open < 0 {
			c.t.Fatalf("%s: unexpected line %q", c.name, line)
		}
		seq, err := strconv.Atoi(line[2:open])
		if err != nil {
			c.t.Fatalf("%s: bad seq in %q", c.name, line)
		}
		fr := fetchFrame{seq: seq}
		rest := strings.TrimSpace(line[open+len(" FETCH ("):])
		if strings.HasSuffix(rest, ")") {
			rest = rest[:len(rest)-1]
		}
		// UID <n>
		if !strings.HasPrefix(rest, "UID ") {
			c.t.Fatalf("%s: missing UID in %q", c.name, line)
		}
		rest = rest[4:]
		var head string
		if sp := strings.IndexByte(rest, ' '); sp >= 0 {
			head, rest = rest[:sp], strings.TrimSpace(rest[sp+1:])
		} else {
			head, rest = rest, ""
		}
		if fr.uid, err = strconv.Atoi(head); err != nil {
			c.t.Fatalf("%s: bad uid %q", c.name, head)
		}
		// FLAGS (...) — the inner list is read up to the closing ')'.
		if strings.HasPrefix(rest, "FLAGS") {
			rest = strings.TrimSpace(strings.TrimPrefix(rest, "FLAGS"))
			if len(rest) == 0 || rest[0] != '(' {
				c.t.Fatalf("%s: bad FLAGS in %q", c.name, line)
			}
			close := strings.IndexByte(rest, ')')
			if close < 0 {
				c.t.Fatalf("%s: unterminated FLAGS in %q", c.name, line)
			}
			fr.flags = rest[1:close]
			rest = strings.TrimSpace(rest[close+1:])
		}
		// Zero or more BODY sections, each "SECTION {n}\r\n" + n raw bytes.
		for rest != "" {
			brace := strings.LastIndexByte(rest, '{')
			if !strings.HasPrefix(rest, "BODY") || brace < 0 || rest[len(rest)-1] != '}' {
				c.t.Fatalf("%s: bad body section header %q", c.name, rest)
			}
			section := strings.TrimSpace(rest[:brace])
			n, err := strconv.Atoi(rest[brace+1 : len(rest)-1])
			if err != nil {
				c.t.Fatalf("%s: bad literal count in %q", c.name, rest)
			}
			buf := make([]byte, n)
			if _, err := io.ReadFull(c.br, buf); err != nil {
				c.t.Fatalf("%s: read literal: %v", c.name, err)
			}
			fr.bodies = append(fr.bodies, fetchBody{section: section, data: string(buf)})
			next := c.readLine()
			if next == ")" {
				rest = ""
				continue
			}
			rest = strings.TrimSpace(next)
		}
		out = append(out, fr)
	}
}

func expectSilence(t *testing.T, c *testClient) {
	t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if line, err := c.br.ReadString('\n'); err == nil {
		t.Fatalf("%s: unexpected notification: %q", c.name, line)
	}
	c.conn.SetReadDeadline(time.Time{})
}

// ---- the scenarios described in the bug report ----------------------------

// TestFetchPartialPeekAndSeen covers byte-exact partials over multibyte and
// binary content, BODY.PEEK[] not marking \Seen, multiple body items in one
// command, offsets past the end, multi-range commands, expunge renumbering
// and invalid arguments leaving storage untouched.
func TestFetchPartialPeekAndSeen(t *testing.T) {
	_, addr := startTestServer(t)

	a := newTestClient(t, "A", addr)
	defer a.close()
	b := newTestClient(t, "B", addr)
	defer b.close()

	tag := a.command("SELECT INBOX")
	a.expectLines("* 0 EXISTS", "* OK [UIDNEXT 1] next uid", tag+" OK SELECT completed")
	tag = b.command("SELECT INBOX")
	b.expectLines("* 0 EXISTS", "* OK [UIDNEXT 1] next uid", tag+" OK SELECT completed")

	// Multibyte (Chinese) and raw binary content: exactly 17 bytes.
	// "中文中" = 9 bytes, "-ab-" = 4, then 0x00 0xff 0x41 0x42.
	body := "中文中-ab-" + "\x00\xffAB"
	if len(body) != 17 {
		t.Fatalf("test setup: body is %d bytes, want 17", len(body))
	}
	tag = a.appendMsg("INBOX", "", body)
	a.expectLines(exists(1), appendOK(tag, 1))
	b.expectLines(exists(1))

	// 1) Two partials in one PEEK command, both returned in request order;
	//    ranges are raw-byte ranges, not rune ranges.
	tag = a.command("UID FETCH 1 (BODY.PEEK[]<0.3> BODY.PEEK[]<3.5>)")
	frames := a.readFetchFrames(tag)
	if len(frames) != 1 {
		t.Fatalf("want 1 frame, got %d", len(frames))
	}
	fr := frames[0]
	if fr.seq != 1 || fr.uid != 1 || fr.flags != "" {
		t.Fatalf("unexpected frame header: %+v", fr)
	}
	if len(fr.bodies) != 2 {
		t.Fatalf("want 2 body sections, got %d: %+v", len(fr.bodies), fr.bodies)
	}
	if fr.bodies[0].section != "BODY[]<0>" || fr.bodies[0].data != body[0:3] {
		t.Fatalf("partial 1 wrong: %q %q", fr.bodies[0].section, fr.bodies[0].data)
	}
	if fr.bodies[1].section != "BODY[]<3>" || fr.bodies[1].data != body[3:8] {
		t.Fatalf("partial 2 wrong: %q %q", fr.bodies[1].section, fr.bodies[1].data)
	}
	// PEEK changed no flags and notified nobody.
	expectSilence(t, b)
	tag = a.command("UID FETCH 1 (FLAGS)")
	if got := a.readFetchFrames(tag); len(got) != 1 || got[0].flags != "" {
		t.Fatalf("PEEK must not set Seen: %+v", got)
	}

	// 2) A real read mixed with a PEEK in one command: marked Seen exactly
	//    once; B receives exactly one FLAGS revision.
	tag = a.command("UID FETCH 1 (BODY[]<3.5> BODY.PEEK[]<0.0>)")
	frames = a.readFetchFrames(tag)
	if len(frames) != 1 || len(frames[0].bodies) != 2 {
		t.Fatalf("want one frame with two sections: %+v", frames)
	}
	if frames[0].bodies[0].section != "BODY[]<3>" || frames[0].bodies[0].data != body[3:8] {
		t.Fatalf("body section wrong: %+v", frames[0].bodies[0])
	}
	if frames[0].bodies[1].section != "BODY[]<0>" || frames[0].bodies[1].data != "" {
		t.Fatalf("zero partial wrong: %+v", frames[0].bodies[1])
	}
	if frames[0].flags != `\Seen` {
		t.Fatalf("real read must show Seen in reply: %+v", frames[0])
	}
	b.expectLines(`* 1 FETCH (UID 1 FLAGS (\Seen))`)
	expectSilence(t, b)
	// Reading again changes nothing further.
	tag = a.command("UID FETCH 1 (BODY.PEEK[]<0.1>)")
	a.readFetchFrames(tag)
	expectSilence(t, b)

	// 3) Offset past the end returns zero bytes; over-long count is clamped;
	//    the declared octet count always equals the bytes actually sent.
	tag = a.command("UID FETCH 1 (BODY.PEEK[]<100.5>)")
	if frs := a.readFetchFrames(tag); len(frs) != 1 || len(frs[0].bodies) != 1 ||
		frs[0].bodies[0].section != "BODY[]<100>" || frs[0].bodies[0].data != "" {
		t.Fatalf("past-end partial wrong: %+v", frs)
	}
	tag = a.command("UID FETCH 1 (BODY.PEEK[]<6.1000>)")
	if frs := a.readFetchFrames(tag); len(frs) != 1 ||
		frs[0].bodies[0].section != "BODY[]<6>" || frs[0].bodies[0].data != body[6:] {
		t.Fatalf("clamped partial wrong: %+v", frs[0].bodies)
	}
	tag = a.command("UID FETCH 1 (BODY.PEEK[])")
	if frs := a.readFetchFrames(tag); len(frs) != 1 ||
		frs[0].bodies[0].section != "BODY[]" || frs[0].bodies[0].data != body {
		t.Fatalf("full body wrong: %+v", frs[0].bodies)
	}

	// 4) Multiple UID ranges in one command and renumbering after expunge.
	body2 := "\x01\x02bin2"
	tag = a.appendMsg("INBOX", "", body2)
	a.expectLines(exists(2), appendOK(tag, 2))
	b.expectLines(exists(2))
	tag = a.command("UID FETCH 1:2 (BODY.PEEK[]<0.2>)")
	frames = a.readFetchFrames(tag)
	if len(frames) != 2 {
		t.Fatalf("want two frames, got %+v", frames)
	}
	if frames[0].seq != 1 || frames[0].uid != 1 || frames[0].bodies[0].data != body[:2] {
		t.Fatalf("frame 1 wrong: %+v", frames[0])
	}
	if frames[1].seq != 2 || frames[1].uid != 2 || frames[1].bodies[0].data != body2[:2] {
		t.Fatalf("frame 2 wrong: %+v", frames[1])
	}
	expectSilence(t, b)

	tag = a.command(`UID STORE 2 +FLAGS (\Deleted)`)
	a.expectLines(`* 2 FETCH (UID 2 FLAGS (\Deleted))`, tag+" OK STORE completed")
	b.expectLines(`* 2 FETCH (UID 2 FLAGS (\Deleted))`)
	tag = a.command("EXPUNGE")
	a.expectLines("* 2 EXPUNGE", tag+" OK EXPUNGE completed")
	b.expectLines("* 2 EXPUNGE")
	// uid 1 stays at seq 1; a later append (uid 3) lands at seq 2.
	tag = a.appendMsg("INBOX", "", "third")
	a.expectLines(exists(2), appendOK(tag, 3))
	b.expectLines(exists(2))
	tag = a.command("UID FETCH 1:3 (FLAGS)")
	frames = a.readFetchFrames(tag)
	if len(frames) != 2 || frames[0].seq != 1 || frames[0].uid != 1 ||
		frames[1].seq != 2 || frames[1].uid != 3 {
		t.Fatalf("renumbered frames wrong: %+v", frames)
	}
	// Expunged uid 2 is unreachable.
	tag = a.command("UID FETCH 2 (BODY.PEEK[])")
	if frs := a.readFetchFrames(tag); len(frs) != 0 {
		t.Fatalf("expunged UID must match nothing: %+v", frs)
	}

	// 5) Illegal parameters: BAD, no frames, no flag mutation, no broadcast.
	for _, bad := range []string{
		"UID FETCH 1 (BODY)",
		"UID FETCH 1 (ENVELOPE)",
		"UID FETCH 1 (BODY.PEEK[]<x.2>)",
		"UID FETCH 1 (BODY[]<->)",
		"UID FETCH 1 FLAGS",
		"UID FETCH 0 (BODY.PEEK[])",
		"UID FETCH 1 (BODY.PEEK[]<0.>)",
	} {
		tag = a.nextTag()
		a.send(tag + " " + bad + "\r\n")
		line := a.readLine()
		if !strings.HasPrefix(line, tag+" BAD") {
			t.Fatalf("%q -> %q, want BAD", bad, line)
		}
	}
	expectSilence(t, b)
	// uid 3 must still be unseen after all the rejected BODY commands.
	tag = a.command("UID FETCH 3 (FLAGS)")
	if frs := a.readFetchFrames(tag); len(frs) != 1 || frs[0].flags != "" {
		t.Fatalf("rejected FETCH must not mutate flags: %+v", frs)
	}
}

// TestAtomicFlagWrites verifies that a flag write failing midway through a
// multi-message command changes nothing: the store rolls back, the reply is
// NO without untagged data, and the other selected session gets no frames.
func TestAtomicFlagWrites(t *testing.T) {
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
	t.Cleanup(func() { srv.Close(); store.Close() })
	addr := ln.Addr().String()

	a := newTestClient(t, "A", addr)
	defer a.close()
	b := newTestClient(t, "B", addr)
	defer b.close()
	tag := a.command("SELECT INBOX")
	a.expectLines("* 0 EXISTS", "* OK [UIDNEXT 1] next uid", tag+" OK SELECT completed")
	tag = b.command("SELECT INBOX")
	b.expectLines("* 0 EXISTS", "* OK [UIDNEXT 1] next uid", tag+" OK SELECT completed")

	for i, body := range []string{"m1", "m2", "m3"} {
		tag = a.appendMsg("INBOX", "", body)
		uid := i + 1
		a.expectLines(exists(uid), appendOK(tag, uid))
		b.expectLines(exists(uid))
	}

	// A trigger that aborts every flags UPDATE except the first message's,
	// simulating a storage error midway through a multi-message batch.
	if _, err := store.db.Exec(`CREATE TRIGGER fail_second_flags
UPDATE OF flags ON messages
WHEN old.uid > (SELECT MIN(uid) FROM messages WHERE mailbox = 'INBOX')
BEGIN
	SELECT RAISE(ABORT, 'injected write failure');
END;`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}

	// STORE covering uids 1..3: the second write fails -> whole command rolls.
	tag = a.command(`UID STORE 1:3 +FLAGS (\Deleted)`)
	if line := a.readLine(); line != tag+" NO storage error" {
		t.Fatalf("got %q, want %q", line, tag+" NO storage error")
	}
	expectSilence(t, b)

	// A real FETCH over several unseen messages hits the same failure: no
	// FETCH frame is emitted and no \Seen persists anywhere.
	tag = a.command("UID FETCH 1:3 (BODY.PEEK[]<0.1>)") // peek first: no writes
	if peekFrames := a.readFetchFrames(tag); len(peekFrames) != 3 {
		t.Fatalf("peek read should still return 3 frames: %+v", peekFrames)
	}
	tag = a.command("UID FETCH 1:3 (BODY[]<0.1>)")
	if line := a.readLine(); line != tag+" NO storage error" {
		t.Fatalf("got %q, want %q", line, tag+" NO storage error")
	}
	expectSilence(t, b)

	// Clear the fault: connection and storage are healthy, flags untouched.
	if _, err := store.db.Exec(`DROP TRIGGER fail_second_flags`); err != nil {
		t.Fatalf("drop trigger: %v", err)
	}
	tag = a.command("UID FETCH 1:3 (FLAGS)")
	frames := a.readFetchFrames(tag)
	if len(frames) != 3 {
		t.Fatalf("want 3 frames: %+v", frames)
	}
	for i, fr := range frames {
		if fr.seq != i+1 || fr.uid != i+1 || fr.flags != "" {
			t.Fatalf("message flags must be unchanged after failed batches: %+v", fr)
		}
	}
	tag = a.command(`UID STORE 1:3 +FLAGS (\Seen)`)
	a.expectLines(
		`* 1 FETCH (UID 1 FLAGS (\Seen))`,
		`* 2 FETCH (UID 2 FLAGS (\Seen))`,
		`* 3 FETCH (UID 3 FLAGS (\Seen))`,
		tag+" OK STORE completed",
	)
	b.expectLines(
		`* 1 FETCH (UID 1 FLAGS (\Seen))`,
		`* 2 FETCH (UID 2 FLAGS (\Seen))`,
		`* 3 FETCH (UID 3 FLAGS (\Seen))`,
	)
}
