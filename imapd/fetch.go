package imapd

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// fetchItem is one parsed FETCH data item.
type fetchItem struct {
	flags   bool // FLAGS
	body    bool // BODY[] or BODY.PEEK[]
	peek    bool // BODY.PEEK[]: fetch without setting \Seen
	partial bool // a <start.count> byte range was given
	start   uint64
	count   uint64
}

// bodyItemRe matches BODY[], BODY.PEEK[] and their <start.count> partial
// forms (case-insensitively). Ranges are raw byte offsets into the stored
// body — never rune indices — so multi-byte and binary content slice
// exactly.
var bodyItemRe = regexp.MustCompile(`(?i)^BODY(\.PEEK)?\[\](?:<([0-9]+)\.([0-9]+)>)?$`)

// parseFetchItems parses a FETCH item list: "FLAGS", "BODY[]<0.100>",
// "(BODY.PEEK[] FLAGS)" and so on. Anything unknown is rejected so that an
// invalid command never reaches the store.
func parseFetchItems(spec string) ([]fetchItem, error) {
	s := strings.TrimSpace(spec)
	if strings.HasPrefix(s, "(") {
		if !strings.HasSuffix(s, ")") {
			return nil, errSyntax
		}
		s = strings.TrimSpace(s[1 : len(s)-1])
	}
	if s == "" {
		return nil, errSyntax
	}
	var items []fetchItem
	for _, tok := range strings.Fields(s) {
		if strings.EqualFold(tok, "FLAGS") {
			items = append(items, fetchItem{flags: true})
			continue
		}
		m := bodyItemRe.FindStringSubmatch(tok)
		if m == nil {
			return nil, errSyntax
		}
		it := fetchItem{body: true, peek: m[1] != ""}
		if m[2] != "" {
			start, err := strconv.ParseUint(m[2], 10, 64)
			if err != nil {
				return nil, errSyntax
			}
			count, err := strconv.ParseUint(m[3], 10, 64)
			if err != nil {
				return nil, errSyntax
			}
			it.partial, it.start, it.count = true, start, count
		}
		items = append(items, it)
	}
	return items, nil
}

// sliceBody extracts a BODY item's bytes from the raw stored body. A range
// starting beyond the end of the body yields zero bytes; a range running
// past the end is truncated to what remains.
func sliceBody(body []byte, it fetchItem) []byte {
	if !it.partial {
		return body
	}
	if it.start >= uint64(len(body)) {
		return nil
	}
	start := int(it.start)
	n := it.count
	if avail := uint64(len(body) - start); n > avail {
		n = avail
	}
	return body[start : start+int(n)]
}

// responseName is how the item appears in the untagged FETCH response: the
// PEEK marker is dropped (it only governs the \Seen side effect) and a
// partial echoes its origin offset, per IMAP convention.
func (it fetchItem) responseName() string {
	if it.partial {
		return fmt.Sprintf("BODY[]<%d>", it.start)
	}
	return "BODY[]"
}

// cmdFetch handles UID FETCH. BODY[] marks each fetched message \Seen — one
// flag revision for the whole command, persisted atomically and notified to
// the other selected sessions in mailbox revision order — while BODY.PEEK[]
// never changes flags. Byte ranges apply to the raw body octets, and every
// requested body item is returned in request order.
func (s *Session) cmdFetch(tag, args string) {
	h, box := s.selected()
	if h == nil {
		s.sendf("%s BAD no mailbox selected", tag)
		return
	}
	spec, itemSpec, ok := cutSpace(args)
	if !ok {
		s.sendf("%s BAD invalid FETCH", tag)
		return
	}
	// Validate the items before touching any state: an invalid field or
	// range must not change the store.
	items, err := parseFetchItems(itemSpec)
	if err != nil {
		s.sendf("%s BAD invalid FETCH items", tag)
		return
	}
	var bodyItems []fetchItem
	markSeen := false
	for _, it := range items {
		if it.body {
			bodyItems = append(bodyItems, it)
			if !it.peek {
				markSeen = true
			}
		}
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	msgs, err := s.srv.store.List(box, len(bodyItems) > 0)
	if err != nil {
		s.sendf("%s NO storage error", tag)
		return
	}
	set, err := parseUIDSet(spec, maxUID(msgs))
	if err != nil {
		s.sendf("%s BAD invalid UID set", tag)
		return
	}

	// Sequence numbers are positions in the current UID ordering, computed
	// under the hub lock, so the responses and the notifications agree even
	// after an EXPUNGE renumbered the mailbox.
	var updates []FlagUpdate
	var responses, notices [][]byte
	for i, m := range msgs {
		if !uidInSet(set, m.UID) {
			continue
		}
		seq := i + 1
		flags := m.Flags
		changed := false
		if markSeen && !hasFlag(flags, `\Seen`) {
			flags = applyFlagOp(flags, []string{`\Seen`}, 1)
			changed = true
		}
		var b bytes.Buffer
		fmt.Fprintf(&b, "* %d FETCH (UID %d FLAGS (%s)", seq, m.UID, joinFlags(flags))
		for _, it := range bodyItems {
			data := sliceBody(m.Body, it)
			fmt.Fprintf(&b, " %s {%d}\r\n", it.responseName(), len(data))
			b.Write(data)
		}
		b.WriteString(")\r\n")
		responses = append(responses, b.Bytes())
		if changed {
			updates = append(updates, FlagUpdate{UID: m.UID, Flags: flags})
			notices = append(notices, []byte(fmt.Sprintf("* %d FETCH (UID %d FLAGS (%s))\r\n",
				seq, m.UID, joinFlags(flags))))
		}
	}
	// The \Seen changes are a single mailbox revision: persist them
	// atomically before anyone is notified, so a mid-way failure leaves no
	// half-written flags and no half-sent notifications behind.
	if err := s.srv.store.SetFlagsAll(box, updates); err != nil {
		s.sendf("%s NO storage error", tag)
		return
	}
	for _, f := range responses {
		s.enqueue(f)
	}
	// The originator already sees the new flags in its FETCH responses; the
	// other selected sessions get one untagged update per changed message.
	h.broadcast(s, true, notices...)
	s.sendf("%s OK FETCH completed", tag)
}
