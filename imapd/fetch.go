package imapd

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

func (s *Session) cmdDetailedFetch(tag, args string) {
	h, box := s.selected()
	if h == nil {
		s.sendf("%s BAD no mailbox selected", tag)
		return
	}
	spec, items, ok := cutSpace(args)
	if !ok {
		s.sendf("%s BAD invalid FETCH", tag)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	msgs, err := s.srv.store.List(box, true)
	if err != nil {
		s.sendf("%s NO storage error", tag)
		return
	}
	set, err := parseUIDSet(spec, maxUID(msgs))
	if err != nil {
		s.sendf("%s BAD invalid UID set", tag)
		return
	}
	body := strings.Contains(strings.ToUpper(items), "BODY")
	for i, m := range msgs {
		if !uidInSet(set, m.UID) {
			continue
		}
		data := m.Body
		if body {
			m.Flags = applyFlagOp(m.Flags, []string{`\Seen`}, 1)
			s.srv.store.SetFlags(box, m.UID, m.Flags)
			partial := regexp.MustCompile(`<([0-9]+)\.([0-9]+)>`).FindStringSubmatch(items)
			if partial != nil {
				start, _ := strconv.Atoi(partial[1])
				count, _ := strconv.Atoi(partial[2])
				runes := []rune(string(data))
				if start < len(runes) {
					end := start + count
					if end > len(runes) {
						end = len(runes)
					}
					data = []byte(string(runes[start:end]))
				} else {
					data = nil
				}
			}
			frame := []byte(fmt.Sprintf("* %d FETCH (UID %d FLAGS (%s) BODY[] {%d}\r\n", i+1, m.UID, joinFlags(m.Flags), len([]rune(string(data)))))
			frame = append(frame, data...)
			frame = append(frame, []byte(")\r\n")...)
			s.enqueue(frame)
		} else {
			s.sendf("* %d FETCH (UID %d FLAGS (%s))", i+1, m.UID, joinFlags(m.Flags))
		}
	}
	s.sendf("%s OK FETCH completed", tag)
}
