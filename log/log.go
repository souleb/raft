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
type LogEntries struct {
	// entries holds the log entries.
	entries []*LogEntry
	// start is the index of the first entry in the log.
	start uint64
}

func New(idx uint64, entries []*LogEntry) LogEntries {
	l := LogEntries{
		start: idx,
	}
	l.entries = make([]*LogEntry, len(entries))
	if len(entries) > 0 {
		copy(l.entries, entries)
	}
	return l
}

func (l LogEntries) Start() uint64 {
	return l.start
}

func (l LogEntries) Length() int {
	return len(l.entries)
}

func (l LogEntries) LastIndex() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.Last().Index
}

// Last returns the last entry in the log.
func (l LogEntries) Last() *LogEntry {
	if len(l.entries) == 0 {
		return nil
	}
	return l.entries[len(l.entries)-1]
}

// LastTerm returns the Term of the last entry in the log.
func (l LogEntries) LastTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.Last().Term
}

// LastCommand returns the Command of the last entry in the log.
func (l LogEntries) LastCommand() any {
	if len(l.entries) == 0 {
		return nil
	}
	return l.Last().Command
}

// LastSN returns the SN of the last entry in the log.
func (l LogEntries) LastSN() int64 {
	if len(l.entries) == 0 {
		return -1
	}
	return l.Last().Sn
}

// GetLogTerm returns the Term of the entry at the given index.
func (l LogEntries) GetLogTerm(index uint64) uint64 {
	if len(l.entries) == 0 || index > l.LastIndex() || index < l.start {
		return 0
	}
	return l.entries[index-l.start].Term
}

// GetLog returns the entry at the given index.
func (l LogEntries) GetLog(index uint64) *LogEntry {
	if len(l.entries) == 0 || index > l.LastIndex() || index < l.start {
		return nil
	}
	return l.entries[index-l.start]
}

// AppendEntry appends an entry to the log.
func (l *LogEntries) AppendEntry(entry *LogEntry) {
	// save the index of the entry in the log
	entry.Index = l.LastIndex() + 1
	l.entries = append(l.entries, entry)
}

// GetEntries returns the entries between the given indexes.
func (l *LogEntries) GetEntries(minIndex, maxIndex uint64) []*LogEntry {
	if minIndex > maxIndex {
		return nil
	}
	if minIndex > l.LastIndex() || minIndex < l.start {
		return nil
	}
	if maxIndex > l.LastIndex() {
		maxIndex = l.LastIndex()
	}
	return l.entries[minIndex-l.start : maxIndex-l.start]
}

// GetEntriesSlice returns the entries between the given indexes.
func (l *LogEntries) GetEntriesSlice(index uint64) []*LogEntry {
	if len(l.entries) == 0 || index > l.LastIndex() || index < l.start {
		return nil
	}
	res := make([]*LogEntry, l.LastIndex()-index+1)
	for i := index; i <= l.LastIndex(); i++ {
		res[i-index] = l.entries[i-l.start]
	}
	return res
}

// GetEntriesFromIndex returns the entries from the given index.
func (l *LogEntries) GetEntriesFromIndex(index uint64) []*LogEntry {
	if len(l.entries) == 0 || index > l.LastIndex() || index < l.start {
		return nil
	}
	return l.entries[index-l.start:]
}

// MatchEntry returns true if the given prevLogIndex and prevLogTerm match the log.
func (l *LogEntries) MatchEntry(prevLogIndex uint64, prevLogTerm uint64) (bool, uint64, uint64) {
	if len(l.entries) == 0 || prevLogIndex > l.LastIndex() || prevLogIndex < l.start {
		return false, 0, 0
	}

	if prevLogIndex > l.LastIndex() {
		return false, l.LastIndex(), l.LastTerm()
	}

	term := l.GetLogTerm(prevLogIndex)

	ok := term == prevLogTerm

	if !ok {
		// get the index of the first entry with the conflicting term
		var i uint64
		conflictTerm := term
		for i = prevLogIndex - 1; i >= l.start; i-- {
			if l.GetLogTerm(i) != conflictTerm {
				break
			}
		}
		return ok, i + 1, conflictTerm
	}

	return ok, prevLogIndex, term
}

func (l *LogEntries) GetConflictIndex(start, index uint64, term uint64) (bool, uint64) {
	if l == nil || index > l.LastIndex() {
		return false, 0
	}

	for i := start; i >= l.start; i-- {
		if l.GetLogTerm(i) == term {
			return true, i
		}
	}
	return false, 0
}

// StoreEntriesFromIndex stores the entries from the given index.
// If the existing entry conflicts with a new one (same index but different terms),
// delete the existing entry and all that follow it and append the new entries.
// If the new entry is not in the existing log, append it.
func (l *LogEntries) StoreEntriesFromIndex(index uint64, entries []*LogEntry) {
	// fill in the index of the entries
	setIndexes(entries, index)

	for i, entry := range entries {
		if int(l.LastIndex()) > int(index)+i {
			// if existing entry conflicts with new one (same index but different terms),
			// delete the existing entry and all that follow it and append the new entries
			// then return
			if l.entries[index-l.start+uint64(i)].Term != entry.Term {
				l.entries = append(l.entries[:index-l.start+uint64(i)], entries[i:]...)
				return
			}
		}

		// if new entry is not in the existing log, append it
		l.AppendEntry(entry)
	}
}

func setIndexes(entries []*LogEntry, index uint64) {
	for i, entry := range entries {
		entry.Index = index + uint64(i)
	}
}

// GetAllEntries returns all the entries in the log.
func (l LogEntries) GetAllEntries() []*LogEntry {
	return l.entries
}

// DeleteEntriesBefore deletes all the entries before the given index including
// the entry at the given index.
func (l *LogEntries) DeleteEntriesBefore(index uint64) {
	if index < l.start {
		return
	}
	l.entries = l.entries[index-l.start+1:]
	l.start = index + 1
}
