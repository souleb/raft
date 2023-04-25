package log

import "fmt"

// LogEntry is a log entry.
type LogEntry struct {
	// Term is the Term in which the entry was received by the leader.
	Term int64
	// Command is the Command to be applied to the state machine.
	Command []byte
	// sn is the serial number of the entry.
	Sn int64
}

func (l *LogEntry) String() string {
	return fmt.Sprintf("Term: %d, Command: %s", l.Term, l.Command)
}

// LogEntries is a slice of LogEntry.
type LogEntries []LogEntry

func (l LogEntries) LastIndex() int64 {
	return int64(len(l) - 1)
}

func (l LogEntries) Last() *LogEntry {
	if len(l) == 0 {
		return nil
	}
	return &l[l.LastIndex()]
}

func (l LogEntries) LastTerm() int64 {
	if len(l) == 0 {
		return -1
	}
	return l.Last().Term
}

func (l LogEntries) LastCommand() any {
	if len(l) == 0 {
		return nil
	}
	return l.Last().Command
}

func (l LogEntries) LastSN() int64 {
	if len(l) == 0 {
		return -1
	}
	return l.Last().Sn
}

func (l LogEntries) GetLogTerm(index int64) int64 {
	return l[index].Term
}

func (l *LogEntries) AppendEntry(entry LogEntry) {
	*l = append(*l, entry)
}

func (l *LogEntries) AppendEntries(entries LogEntries) {
	*l = append(*l, entries...)
}

func (l *LogEntries) GetEntriesFromIndex(index int64) LogEntries {
	return (*l)[index:]
}

func (l *LogEntries) GetLastLogIndexAndTerm() (int64, int64) {
	return l.LastIndex(), l.LastTerm()
}

func (l *LogEntries) MatchEntry(prevLogIndex, prevLogTerm int64) bool {
	if l == nil || prevLogIndex > l.LastIndex() {
		return false
	}

	return l.GetLogTerm(prevLogIndex) == prevLogTerm
}

// StoreEntriesFromIndex stores the entries from the given index.
// If the existing entry conflicts with a new one (same index but different terms),
// delete the existing entry and all that follow it and append the new entries.
// If the new entry is not in the existing log, append it.
func (l *LogEntries) StoreEntriesFromIndex(index int64, entries LogEntries) {
	for i, entry := range entries {
		if len(*l) > int(index)+i {
			// if existing entry conflicts with new one (same index but different terms),
			// delete the existing entry and all that follow it and append the new entries
			// then return
			if (*l)[index+int64(i)].Term != entry.Term {
				*l = append((*l)[:index+int64(i)], entries[i:]...)
				return
			}
			continue
		}

		// if new entry is not in the existing log, append it
		*l = append(*l, entry)
	}
}
