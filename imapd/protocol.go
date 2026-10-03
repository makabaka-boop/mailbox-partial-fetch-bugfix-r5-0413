package imapd

import (
	"bufio"
	"errors"
	"sort"
	"strconv"
	"strings"
)

var errSyntax = errors.New("syntax error")

// readLine reads one CRLF- (or LF-) terminated command line. It tolerates the
// line arriving split across any number of TCP segments and rejects lines
// longer than maxLineLength so a client cannot exhaust memory.
func readLine(br *bufio.Reader) (string, error) {
	var buf []byte
	for {
		frag, err := br.ReadSlice('\n')
		buf = append(buf, frag...)
		if err == bufio.ErrBufferFull {
			if len(buf) > maxLineLength {
				return "", errors.New("command line too long")
			}
			continue
		}
		if err != nil {
			return "", err
		}
		if len(buf) > maxLineLength {
			return "", errors.New("command line too long")
		}
		break
	}
	line := strings.TrimSuffix(string(buf), "\n")
	line = strings.TrimSuffix(line, "\r")
	return line, nil
}

// cutSpace splits s at the first run of spaces into head and the trimmed rest.
func cutSpace(s string) (head, rest string, ok bool) {
	i := strings.IndexByte(s, ' ')
	if i < 0 {
		return s, "", false
	}
	return s[:i], strings.TrimLeft(s[i+1:], " "), true
}

// parseAppendArgs parses: <mailbox> [ (flags) ] {<size>}  (a trailing '+' on
// the literal size, LITERAL+, is accepted and ignored).
func parseAppendArgs(args string) (box string, flags []string, size int, err error) {
	if !strings.HasSuffix(args, "}") {
		return "", nil, 0, errSyntax
	}
	open := strings.LastIndexByte(args, '{')
	if open < 0 {
		return "", nil, 0, errSyntax
	}
	num := strings.TrimSuffix(args[open+1:len(args)-1], "+")
	n, err := strconv.ParseUint(num, 10, 31)
	if err != nil {
		return "", nil, 0, errSyntax
	}
	prefix := strings.TrimSpace(args[:open])
	if prefix == "" {
		return "", nil, 0, errSyntax
	}
	if i := strings.IndexByte(prefix, ' '); i >= 0 {
		box = prefix[:i]
		if flags, err = parseFlagList(strings.TrimSpace(prefix[i+1:])); err != nil {
			return "", nil, 0, err
		}
	} else {
		box = prefix
	}
	if box == "" {
		return "", nil, 0, errSyntax
	}
	return box, flags, int(n), nil
}

// uidInterval is an inclusive UID range.
type uidInterval struct{ lo, hi uint64 }

// parseUIDSet parses sets like "1,3,5:*" or "*". "*" resolves to maxUID, the
// highest UID currently in the mailbox (0 when empty, which matches nothing).
func parseUIDSet(spec string, maxUID uint64) ([]uidInterval, error) {
	if spec == "" {
		return nil, errSyntax
	}
	var out []uidInterval
	for _, part := range strings.Split(spec, ",") {
		if part == "" {
			return nil, errSyntax
		}
		if i := strings.IndexByte(part, ':'); i >= 0 {
			a, err := parseUIDValue(part[:i], maxUID)
			if err != nil {
				return nil, err
			}
			b, err := parseUIDValue(part[i+1:], maxUID)
			if err != nil {
				return nil, err
			}
			if a > b {
				a, b = b, a
			}
			out = append(out, uidInterval{a, b})
		} else {
			v, err := parseUIDValue(part, maxUID)
			if err != nil {
				return nil, err
			}
			out = append(out, uidInterval{v, v})
		}
	}
	return out, nil
}

func parseUIDValue(s string, maxUID uint64) (uint64, error) {
	if s == "*" {
		return maxUID, nil
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || n == 0 {
		return 0, errSyntax
	}
	return n, nil
}

func uidInSet(set []uidInterval, uid uint64) bool {
	for _, iv := range set {
		if uid >= iv.lo && uid <= iv.hi {
			return true
		}
	}
	return false
}

// parseFlagList parses "(\\Seen \\Deleted)" or a bare "\\Seen \\Deleted".
func parseFlagList(s string) ([]string, error) {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "(") {
		if !strings.HasSuffix(s, ")") {
			return nil, errSyntax
		}
		s = s[1 : len(s)-1]
	}
	fields := strings.Fields(s)
	for _, f := range fields {
		if strings.ContainsAny(f, "()") {
			return nil, errSyntax
		}
	}
	return fields, nil
}

// normalizeFlags deduplicates and sorts flags so responses are deterministic.
func normalizeFlags(flags []string) []string {
	seen := make(map[string]bool, len(flags))
	out := make([]string, 0, len(flags))
	for _, f := range flags {
		if !seen[f] {
			seen[f] = true
			out = append(out, f)
		}
	}
	sort.Strings(out)
	return out
}

func hasFlag(flags []string, want string) bool {
	for _, f := range flags {
		if strings.EqualFold(f, want) {
			return true
		}
	}
	return false
}

// applyFlagOp applies a STORE operation: mode 0 = replace, 1 = add, 2 = remove.
func applyFlagOp(cur, op []string, mode int) []string {
	switch mode {
	case 0:
		return normalizeFlags(op)
	case 1:
		return normalizeFlags(append(append([]string{}, cur...), op...))
	default:
		out := make([]string, 0, len(cur))
		for _, f := range cur {
			if !hasFlag(op, f) {
				out = append(out, f)
			}
		}
		return normalizeFlags(out)
	}
}
