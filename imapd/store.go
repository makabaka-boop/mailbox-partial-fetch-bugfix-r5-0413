package imapd

import (
	"database/sql"
	"errors"
	"strings"

	_ "modernc.org/sqlite"
)

// ErrMailboxFull is returned by Store.Append when the mailbox already holds
// MaxMessages messages.
var ErrMailboxFull = errors.New("mailbox full")

// Message is one stored message. Body is only populated when requested.
type Message struct {
	UID   uint64
	Flags []string
	Body  []byte
}

// Store persists mailboxes and messages in SQLite. UIDs come from a
// per-mailbox next_uid counter that only ever increases, so a UID is never
// reused — not even after the message is expunged or the server restarts.
// Sequence numbers are deliberately NOT stored: they are always derived as
// the 1-based position in "ORDER BY uid" at query time, so deletions simply
// renumber the remaining messages.
type Store struct {
	db *sql.DB
}

// OpenStore opens (and if necessary creates) the SQLite database at path.
func OpenStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// All callers already serialize per mailbox; a single connection avoids
	// SQLITE_BUSY and keeps cross-mailbox operations ordered too.
	db.SetMaxOpenConns(1)
	// Transactions stay atomic; NORMAL merely skips the per-commit fsync of
	// the journal, which would dominate append-heavy workloads. WAL is best
	// effort (not all filesystems support it).
	db.Exec(`PRAGMA journal_mode = WAL`)
	if _, err := db.Exec(`PRAGMA synchronous = NORMAL`); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS mailboxes (
    name     TEXT PRIMARY KEY,
    next_uid INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
    mailbox TEXT NOT NULL,
    uid     INTEGER NOT NULL,
    flags   TEXT NOT NULL DEFAULT '',
    body    BLOB NOT NULL,
    PRIMARY KEY (mailbox, uid)
);`); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close closes the underlying database.
func (s *Store) Close() error { return s.db.Close() }

func joinFlags(flags []string) string { return strings.Join(flags, " ") }

func splitFlags(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return strings.Fields(s)
}

// MailboxInfo returns the message count and the next UID of box, creating the
// mailbox row if it does not exist yet.
func (s *Store) MailboxInfo(box string) (count int, nextUID uint64, err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`INSERT OR IGNORE INTO mailboxes(name, next_uid) VALUES(?, 1)`, box); err != nil {
		return 0, 0, err
	}
	if err = tx.QueryRow(`SELECT COUNT(*) FROM messages WHERE mailbox = ?`, box).Scan(&count); err != nil {
		return 0, 0, err
	}
	if err = tx.QueryRow(`SELECT next_uid FROM mailboxes WHERE name = ?`, box).Scan(&nextUID); err != nil {
		return 0, 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, 0, err
	}
	return count, nextUID, nil
}

// Append stores body as a new message in box and returns the assigned UID and
// the new message count. The whole operation is one transaction: either the
// message is stored completely or not at all.
func (s *Store) Append(box string, flags []string, body []byte) (uid uint64, count int, err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`INSERT OR IGNORE INTO mailboxes(name, next_uid) VALUES(?, 1)`, box); err != nil {
		return 0, 0, err
	}
	if err = tx.QueryRow(`SELECT COUNT(*) FROM messages WHERE mailbox = ?`, box).Scan(&count); err != nil {
		return 0, 0, err
	}
	if count >= MaxMessages {
		return 0, 0, ErrMailboxFull
	}
	if err = tx.QueryRow(`SELECT next_uid FROM mailboxes WHERE name = ?`, box).Scan(&uid); err != nil {
		return 0, 0, err
	}
	if _, err = tx.Exec(`INSERT INTO messages(mailbox, uid, flags, body) VALUES(?, ?, ?, ?)`,
		box, uid, joinFlags(flags), body); err != nil {
		return 0, 0, err
	}
	if _, err = tx.Exec(`UPDATE mailboxes SET next_uid = ? WHERE name = ?`, uid+1, box); err != nil {
		return 0, 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, 0, err
	}
	return uid, count + 1, nil
}

// List returns all messages of box ordered by UID (i.e. in sequence-number
// order). Pass withBody=false when only metadata is needed.
func (s *Store) List(box string, withBody bool) ([]Message, error) {
	query := `SELECT uid, flags FROM messages WHERE mailbox = ? ORDER BY uid`
	if withBody {
		query = `SELECT uid, flags, body FROM messages WHERE mailbox = ? ORDER BY uid`
	}
	rows, err := s.db.Query(query, box)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Message
	for rows.Next() {
		var m Message
		var flags string
		if withBody {
			if err := rows.Scan(&m.UID, &flags, &m.Body); err != nil {
				return nil, err
			}
		} else if err := rows.Scan(&m.UID, &flags); err != nil {
			return nil, err
		}
		m.Flags = splitFlags(flags)
		out = append(out, m)
	}
	return out, rows.Err()
}

// SetFlags replaces the flag set of one message.
func (s *Store) SetFlags(box string, uid uint64, flags []string) error {
	_, err := s.db.Exec(`UPDATE messages SET flags = ? WHERE mailbox = ? AND uid = ?`,
		joinFlags(flags), box, uid)
	return err
}

// Delete removes the given UIDs in a single transaction.
func (s *Store) Delete(box string, uids []uint64) error {
	if len(uids) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`DELETE FROM messages WHERE mailbox = ? AND uid = ?`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, uid := range uids {
		if _, err := stmt.Exec(box, uid); err != nil {
			return err
		}
	}
	return tx.Commit()
}
