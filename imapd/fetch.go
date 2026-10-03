package imapd

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// FETCH data item kinds.
const (
	itemFlags = iota
	itemBody
)

// fetchItem is one requested FETCH data item: FLAGS, BODY[] or BODY.PEEK[],
// the last two optionally carrying a byte partial <offset.count>.
type fetchItem struct {
	kind       int
	peek       bool // BODY.PEEK[] does not set \Seen
	hasPartial bool
	offset     uint64 // byte offset into the raw body
	count      uint64 // requested byte count (clamped at the end of the body)
}

// parseFetchItems parses the parenthesized item list, e.g.
// "(FLAGS BODY.PEEK[]<0.10> BODY[]<20.5>)". Items contain no spaces and are
// returned in request order. Any unknown or malformed item is a syntax error.
func parseFetchItems(spec string) ([]fetchItem, error) {
	spec = strings.TrimSpace(spec)
	if len(spec) < 2 || spec[0] != '(' || spec[len(spec)-1] != ')' {
		return nil, errSyntax
	}
	tokens := strings.Fields(spec[1 : len(spec)-1])
	if len(tokens) == 0 {
		return nil, errSyntax
	}
	items := make([]fetchItem, 0, len(tokens))
	for _, tok := range tokens {
		up := strings.ToUpper(tok)
		switch {
		case up == "FLAGS":
			items = append(items, fetchItem{kind: itemFlags})
		case strings.HasPrefix(up, "BODY"):
			it, err := parseBodyItem(up)
			if err != nil {
				return nil, err
			}
			items = append(items, it)
		default:
			return nil, errSyntax
		}
	}
	return items, nil
}

// parseBodyItem parses BODY[] / BODY.PEEK[], optionally followed by
// <offset.count>. Both numbers are decimal octet counts.
func parseBodyItem(tok string) (fetchItem, error) {
	it := fetchItem{kind: itemBody}
	var rest string
	switch {
	case strings.HasPrefix(tok, "BODY.PEEK[]"):
		it.peek = true
		rest = tok[len("BODY.PEEK[]"):]
	case strings.HasPrefix(tok, "BODY[]"):
		rest = tok[len("BODY[]"):]
	default:
		return fetchItem{}, errSyntax
	}
	if rest == "" {
		return it, nil
	}
	if len(rest) < 5 || rest[0] != '<' || rest[len(rest)-1] != '>' {
		return fetchItem{}, errSyntax
	}
	dot := strings.IndexByte(rest, '.')
	if dot <= 1 || dot == len(rest)-2 {
		return fetchItem{}, errSyntax
	}
	off, err := strconv.ParseUint(rest[1:dot], 10, 64)
	if err != nil {
		return fetchItem{}, errSyntax
	}
	cnt, err := strconv.ParseUint(rest[dot+1:len(rest)-1], 10, 64)
	if err != nil {
		return fetchItem{}, errSyntax
	}
	it.hasPartial, it.offset, it.count = true, off, cnt
	return it, nil
}

// bodyPartial returns the raw-octet slice of body described by the partial
// spec. Offsets are byte offsets, never rune offsets, so binary and multibyte
// content are handled exactly. An offset at or past the end yields zero
// bytes; a count running past the end is clamped to what remains.
func bodyPartial(body []byte, offset, count uint64) []byte {
	if offset >= uint64(len(body)) {
		return []byte{}
	}
	out := body[offset:]
	if count < uint64(len(out)) {
		out = out[:count]
	}
	return out
}

// cmdDetailedFetch implements UID FETCH with FLAGS, BODY[] and BODY.PEEK[]
// items, each with an optional <offset.count> byte partial.
//
// The whole command is one mailbox revision: arguments are validated before
// anything is written, all \Seen updates for non-PEEK body items persist in a
// single transaction, and only then — still holding the mailbox lock — are the
// originator's FETCH frames and the other sessions' FLAGS notifications
// enqueued. On any error nothing is persisted and no frame is sent, so every
// client observes the same effective operation.
func (s *Session) cmdDetailedFetch(tag, args string) {
	h, box := s.selected()
	if h == nil {
		s.sendf("%s BAD no mailbox selected", tag)
		return
	}
	setSpec, itemSpec, ok := cutSpace(args)
	if !ok || itemSpec == "" {
		s.sendf("%s BAD invalid FETCH arguments", tag)
		return
	}
	// Parse every item before touching storage: illegal parameters must not
	// change any flag.
	items, err := parseFetchItems(itemSpec)
	if err != nil {
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

	// Only a real (non-PEEK) BODY[] read marks messages \Seen.
	markSeen := false
	for _, it := range items {
		if it.kind == itemBody && !it.peek {
			markSeen = true
			break
		}
	}

	// Plan the flag updates first (pure computation, no I/O).
	type seenChange struct {
		index int
		flags []string
	}
	var changes []seenChange
	if markSeen {
		for i := range msgs {
			m := &msgs[i]
			if !uidInSet(set, m.UID) || hasFlag(m.Flags, `\Seen`) {
				continue
			}
			changes = append(changes, seenChange{
				index: i,
				flags: applyFlagOp(m.Flags, []string{`\Seen`}, 1),
			})
		}
	}

	// Persist all the \Seen updates atomically; on failure the command is a
	// NO with no side effects and no frames, even for messages handled before
	// the failing one.
	if len(changes) > 0 {
		updates := make([]FlagUpdate, len(changes))
		for j, c := range changes {
			updates[j] = FlagUpdate{UID: msgs[c.index].UID, Flags: c.flags}
		}
		if err := s.srv.store.SetFlagsBatch(box, updates); err != nil {
			s.sendf("%s NO storage error", tag)
			return
		}
		for _, c := range changes {
			msgs[c.index].Flags = c.flags
		}
	}

	changed := make(map[uint64]bool, len(changes))
	for _, c := range changes {
		changed[msgs[c.index].UID] = true
	}

	// Sequence numbers are derived here, under the hub lock, from the current
	// UID ordering, so a message expunged (and the resulting renumbering) can
	// never make a reply refer to the wrong message.
	var frames, notices [][]byte
	for i := range msgs {
		m := &msgs[i]
		if !uidInSet(set, m.UID) {
			continue
		}
		seq := i + 1
		var b bytes.Buffer
		fmt.Fprintf(&b, "* %d FETCH (UID %d FLAGS (%s)", seq, m.UID, joinFlags(m.Flags))
		// Emit every requested body item in request order; multiple body
		// sections share the one FETCH frame.
		for _, it := range items {
			if it.kind != itemBody {
				continue
			}
			data := m.Body
			if it.hasPartial {
				data = bodyPartial(data, it.offset, it.count)
				// Per RFC 3501 the response section carries the origin
				// octet only; the {n} literal declares the octets actually
				// returned, which always equals len(data) — including zero
				// when the origin lies past the end of the body.
				fmt.Fprintf(&b, " BODY[]<%d> {%d}\r\n", it.offset, len(data))
			} else {
				fmt.Fprintf(&b, " BODY[] {%d}\r\n", len(data))
			}
			b.Write(data)
		}
		b.WriteString(")\r\n")
		frames = append(frames, b.Bytes())
		if changed[m.UID] {
			notices = append(notices, []byte(fmt.Sprintf(
				"* %d FETCH (UID %d FLAGS (%s))\r\n",
				seq, m.UID, joinFlags(m.Flags))))
		}
	}

	// Commit (above) and both enqueues happen in one hub-locked critical
	// section, so the other sessions' notifications and this session's FETCH
	// data describe the same mailbox revision, and per-message frames are
	// ordered by sequence number at every client.
	if len(notices) > 0 {
		h.broadcast(s, true, notices...)
	}
	for _, f := range frames {
		s.enqueue(f)
	}
	s.sendf("%s OK FETCH completed", tag)
}
