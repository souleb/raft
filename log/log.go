package log

import "fmt"

// LogEntry is a log entry.
type LogEntry struct {
	// Index is the index of the entry in the log.
	Index uint64
	// Term is the Term in which the entry was received by the leader.
	Term uint64
	// Command is the Command to be applied to the state machine.
	Command []byte
	// Sn is the serial number of the entry.
	Sn int64
}

// String returns a string representation of the log entry.
// We use a concatinated string of the Term and Command.
func (l *LogEntry) String() string {
	return fmt.Sprintf("Term: %d, Command: %s", l.Term, l.Command)
}

// LogEntries is a slice of LogEntry.
type LogEntries []LogEntry

func (l LogEntries) Length() int {
	return len(l)
}

func (l LogEntries) LastIndex() uint64 {
	if len(l) == 0 {
		return 0
	}
	return uint64(len(l) - 1)
}

// Last returns the last entry in the log.
func (l LogEntries) Last() *LogEntry {
	if len(l) == 0 {
		return nil
	}
	return &l[len(l)-1]
}

// LastTerm returns the Term of the last entry in the log.
func (l LogEntries) LastTerm() uint64 {
	if len(l) == 0 {
		return 0
	}
	return l.Last().Term
}

// LastCommand returns the Command of the last entry in the log.
func (l LogEntries) LastCommand() any {
	if len(l) == 0 {
		return nil
	}
	return l.Last().Command
}

// LastSN returns the SN of the last entry in the log.
func (l LogEntries) LastSN() int64 {
	if len(l) == 0 {
		return -1
	}
	return l.Last().Sn
}

// GetLogTerm returns the Term of the entry at the given index.
func (l LogEntries) GetLogTerm(index uint64) uint64 {
	if len(l) == 0 || index > uint64(len(l)) {
		return 0
	}
	return l[index].Term
}

// GetLog returns the entry at the given index.
func (l LogEntries) GetLog(index uint64) LogEntry {
	if len(l) == 0 || index > uint64(len(l)) {
		return LogEntry{}
	}
	return l[index]
}

// AppendEntry appends an entry to the log.
func (l *LogEntries) AppendEntry(entry LogEntry) {
	// save the index of the entry in the log
	entry.Index = l.LastIndex() + 1
	*l = append(*l, entry)
}

// GetEntries returns the entries between the given indexes.
func (l *LogEntries) GetEntries(minIndex, maxIndex uint64) LogEntries {
	if minIndex > maxIndex {
		return nil
	}
	return (*l)[minIndex:maxIndex]
}

// GetEntriesSlice returns the entries between the given indexes.
func (l *LogEntries) GetEntriesSlice(index uint64) []*LogEntry {
	if index > l.LastIndex() {
		return nil
	}
	res := make([]*LogEntry, l.LastIndex()-index+1)
	for i := index; i <= l.LastIndex(); i++ {
		res[i-index] = &(*l)[i]
	}
	return res
}

// GetEntriesFromIndex returns the entries from the given index.
func (l *LogEntries) GetEntriesFromIndex(index uint64) LogEntries {
	if index > l.LastIndex() {
		return nil
	}
	return (*l)[index:]
}

// MatchEntry returns true if the given prevLogIndex and prevLogTerm match the log.
func (l *LogEntries) MatchEntry(prevLogIndex uint64, prevLogTerm uint64) bool {
	if l == nil || prevLogIndex > l.LastIndex() {
		return false
	}

	return l.GetLogTerm(prevLogIndex) == prevLogTerm
}

// StoreEntriesFromIndex stores the entries from the given index.
// If the existing entry conflicts with a new one (same index but different terms),
// delete the existing entry and all that follow it and append the new entries.
// If the new entry is not in the existing log, append it.
func (l *LogEntries) StoreEntriesFromIndex(index uint64, entries LogEntries) {
	for i, entry := range entries {
		if len(*l) > int(index)+i {
			// if existing entry conflicts with new one (same index but different terms),
			// delete the existing entry and all that follow it and append the new entries
			// then return
			if (*l)[index+uint64(i)].Term != entry.Term {
				*l = append((*l)[:index+uint64(i)], entries[i:]...)
				return
			}
		}

		// if new entry is not in the existing log, append it
		l.AppendEntry(entry)
	}
}
