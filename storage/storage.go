// This is a modified version of raft-faslog by @tidwall, governed by a MOZILLA PUBLIC LICENSE
// provided in the LICENSE file.
// See the original source code at https://github.com/tidwall/raft-fastlog

package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	gerr "errors"

	"github.com/souleb/raft/errors"
	"github.com/souleb/raft/log"
)

// Level is the durability level of the storage
type Level int

const (
	Low Level = iota
	Medium
	High
)

const (
	// minShrinkSize is the minimum size of the file before it is shrunk
	minShrinkSize = 1 << 26 // 64 MB
)

const (
	cmdSet         = '(' // Key+Val
	cmdDel         = ')' // Key
	cmdStoreLogs   = '[' // Count+Log,Log...  Log: Idx+Term+Command
	cmdDeleteRange = ']' // Min+Max
)

// Store is an interface that is used to store the logs
type Store interface {
	// FirstIndex returns the first index written. 0 for no entries.
	FirstIndex() (uint64, error)

	// LastIndex returns the last index written. 0 for no entries.
	LastIndex() (uint64, error)

	// GetLog gets a log entry at a given index.
	GetLog(index uint64) (*log.LogEntry, error)

	// StoreLogs stores multiple log entries. By default the logs stored may not be contiguous with previous logs (i.e. may have a gap in Index since the last log written). If an implementation can't tolerate this it may optionally implement `MonotonicLogStore` to indicate that this is not allowed. This changes Raft's behaviour after restoring a user snapshot to remove all previous logs instead of relying on a "gap" to signal the discontinuity between logs before the snapshot and logs after.
	StoreLogs(logs []*log.LogEntry) error

	// DeleteRange deletes a range of log entries. The range is inclusive.
	DeleteRange(min, max uint64) error

	// Set is used to set a key value pair
	Set(key []byte, val []byte) error

	// Get returns the value for key, or an error.
	Get(key []byte) ([]byte, error)

	// SetUint64 is used to set a key value pair
	SetUint64(key []byte, val uint64) error

	// GetUint64 returns the value for key, or an error.
	GetUint64(key []byte) (uint64, error)
}

// buffer is a helper type used to write data to a byte slice
type buffer []byte

// clear clears the buffer
func (s *buffer) clear() {
	*s = (*s)[:0]
}

// writeByte writes a byte to the buffer
func (s *buffer) writeByte(v byte) {
	*s = append(*s, v)
}

// writeUint64 writes a uint64 to the buffer
// it is written in little endian format
func (s *buffer) writeUint64(v uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	*s = append(*s, buf[:]...)
}

// writeLog writes a log entry to the buffer
func (s *buffer) writeLog(log *log.LogEntry) {
	s.writeUint64(uint64(log.Index))
	s.writeUint64(uint64(log.Term))
	s.writeUint64(uint64(len(log.Command)))
	*s = append(*s, log.Command...)
}

func (s *buffer) writeKV(key, val []byte) {
	*s = append(*s, cmdSet)
	s.writeUint64(uint64(len(key)))
	*s = append(*s, key...)
	s.writeUint64(uint64(len(val)))
	*s = append(*s, val...)
}

var _ Store = (*MemoryStore)(nil)

// MemoryStore is an in-memory implementation of the LogStore interface.
// It can optionally persist the logs to disk.
type MemoryStore struct {
	mu         sync.RWMutex
	path       string
	durability Level
	file       *os.File
	// kv is a map of key to value
	kv map[string][]byte
	// logDB is a map of log index to log entry
	// this is used to store the logs in memory
	logDB map[uint64]*log.LogEntry
	// firstIndex is the index of the first log entry in the store
	firstIndex uint64
	// lastIndex is the index of the last log entry in the store
	lastIndex uint64
	buf       buffer
	// size is the size of the file
	size    int
	dirty   bool
	closed  bool
	persist bool
	// shrinkMark is used to mark if the store is being cycled
	shrinkMark bool
	logger     *slog.Logger
}

// NewStore creates a new MemoryStore
func NewStore(path string, durability Level, logger *slog.Logger) (*MemoryStore, error) {
	if path == "" {
		return nil, &errors.Error{
			StatusCode: errors.InvalidArgument,
			Err:        fmt.Errorf("path cannot be empty"),
		}
	}
	if durability < Low || durability > High {
		return nil, &errors.Error{
			StatusCode: errors.InvalidArgument,
			Err:        fmt.Errorf("invalid durability level"),
		}
	}

	if logger == nil {
		return nil, &errors.Error{
			StatusCode: errors.InvalidArgument,
			Err:        fmt.Errorf("logger cannot be nil"),
		}
	}

	store := &MemoryStore{
		path:       path,
		durability: durability,
		kv:         make(map[string][]byte),
		logDB:      make(map[uint64]*log.LogEntry),
		persist:    path != ":memory:",
		logger:     logger,
	}

	if store.persist {
		f, err := os.OpenFile(store.path, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}
		store.file = f
		err = store.load()
		if err != nil {
			errf := store.file.Close()
			return nil, gerr.Join(err, errf)
		}
		go store.run()
	}
	return store, nil
}

// load loads the logs from the file
func (s *MemoryStore) load() (err error) {
	start := time.Now()
	defer func() {
		if err != nil {
			s.logger.Error("load failed %s", slog.String("duration", time.Since(start).String()))
			return
		}
		s.logger.Info("load completed %s", slog.String("duration", time.Since(start).String()))
	}()

	buf := make([]byte, 8)
	rd := bufio.NewReader(s.file)
	for {
		c, err := rd.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		switch c {
		default:
			return &errors.Error{
				StatusCode: errors.Unknown,
				Err:        fmt.Errorf("unknown command %c", c),
			}
		case cmdSet, cmdDel:
			if _, err := io.ReadFull(rd, buf); err != nil {
				return err
			}
			key := make([]byte, binary.LittleEndian.Uint64(buf))
			if _, err := io.ReadFull(rd, key); err != nil {
				return err
			}
			if c == cmdSet {
				if _, err := io.ReadFull(rd, buf); err != nil {
					return err
				}
				val := make([]byte, binary.LittleEndian.Uint64(buf))
				if _, err := io.ReadFull(rd, val); err != nil {
					return err
				}
				s.kv[string(key)] = val
			} else {
				delete(s.kv, string(key))
			}
		case cmdStoreLogs:
			if _, err := io.ReadFull(rd, buf); err != nil {
				return err
			}
			count := int(binary.LittleEndian.Uint64(buf))
			for i := 0; i < count; i++ {
				log, err := readLogEntry(rd)
				if err != nil {
					return err
				}
				s.updateFirstAndLastIndex(log)
				s.logDB[log.Index] = log
			}
		case cmdDeleteRange:
			if _, err := io.ReadFull(rd, buf); err != nil {
				return err
			}
			min := binary.LittleEndian.Uint64(buf)
			if _, err := io.ReadFull(rd, buf); err != nil {
				return err
			}
			max := binary.LittleEndian.Uint64(buf)
			for i := min; i <= max; i++ {
				delete(s.logDB, i)
			}
			s.fillFirstAndLastIndex()
		}
	}
	pos, err := s.file.Seek(0, 1)
	if err != nil {
		return err
	}
	s.size = int(pos)
	return nil
}

func readLogEntry(rd io.Reader) (*log.LogEntry, error) {
	var (
		buf = make([]byte, 8)
		log = &log.LogEntry{}
	)
	if _, err := io.ReadFull(rd, buf); err != nil {
		return nil, err
	}
	log.Index = binary.LittleEndian.Uint64(buf)
	if _, err := io.ReadFull(rd, buf); err != nil {
		return nil, err
	}
	log.Term = binary.LittleEndian.Uint64(buf)
	if _, err := io.ReadFull(rd, buf); err != nil {
		return nil, err
	}
	size := binary.LittleEndian.Uint32(buf)
	cmd := make([]byte, size)
	if _, err := io.ReadFull(rd, cmd); err != nil {
		return nil, err
	}
	log.Command = buf
	return log, nil
}

// Close closes the store
func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return &errors.Error{
			StatusCode: errors.Closed,
			Err:        fmt.Errorf("failed to get last index from closed storage"),
		}
	}
	s.closed = true
	if s.persist {
		err := s.file.Sync()
		errf := s.file.Close()
		return gerr.Join(err, errf)
	}
	return nil
}

func (s *MemoryStore) run() {
	for {
		time.Sleep(time.Second)
		done := func() bool {
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				return true
			}
			if s.durability == Medium && s.dirty {
				err := s.file.Sync()
				if err != nil {
					s.logger.Error("failed to sync file", slog.String("error", err.Error()))
					s.mu.Unlock()
					return true
				}
				s.dirty = false
			}
			s.mu.Unlock()
			if s.size > minShrinkSize {
				s.Shrink()
			}
			return false
		}()
		if done {
			return
		}
	}
}

// FirstIndex returns the first index written. 0 for no entries.
func (s *MemoryStore) FirstIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, &errors.Error{
			StatusCode: errors.Closed,
			Err:        fmt.Errorf("failed to get last index from closed storage"),
		}
	}
	return s.firstIndex, nil
}

// LastIndex returns the last index written. 0 for no entries.
func (s *MemoryStore) LastIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, &errors.Error{
			StatusCode: errors.Closed,
			Err:        fmt.Errorf("failed to get last index from closed storage"),
		}
	}
	return s.lastIndex, nil
}

// GetLog gets a log entry at a given index.
func (s *MemoryStore) GetLog(index uint64) (*log.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, &errors.Error{
			StatusCode: errors.Closed,
			Err:        fmt.Errorf("failed to get last index from closed storage"),
		}
	}

	if log := s.logDB[index]; log != nil {
		return log, nil
	}

	return nil, &errors.Error{
		StatusCode: errors.NotFound,
		Err:        fmt.Errorf("failed to get log entry at index %d", index),
	}
}

// writeBuf writes the buffer to the file
func (s *MemoryStore) writeBuf() error {
	if _, err := s.file.Write(s.buf); err != nil {
		return err
	}
	s.size += len(s.buf)
	if s.durability == High {
		// If high durability is set, sync the file to disk straight away
		s.file.Sync()
	} else if s.durability == Medium {
		// If medium durability is set, sync the file to disk every second
		// if the size of the file is greater than minShrinkSize
		s.dirty = true
	}
	return nil
}

// SetUint64 is used to set a key value pair
func (s *MemoryStore) SetUint64(key []byte, val uint64) error {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, val)
	return s.Set(key, b)
}

// Set is used to set a key value pair
func (s *MemoryStore) Set(key, val []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return &errors.Error{
			StatusCode: errors.Closed,
			Err:        fmt.Errorf("cannot set key in closed storage"),
		}
	}
	if s.persist {
		s.buf.clear()
		s.buf.writeKV(key, val)
		if err := s.writeBuf(); err != nil {
			return err
		}
	}
	s.kv[string(key)] = val
	return nil
}

// GetUint64 is used to get a value for a given key
func (s *MemoryStore) GetUint64(key []byte) (uint64, error) {
	b, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	if len(b) != 8 {
		return 0, &errors.Error{
			StatusCode: errors.Unknown,
			Err:        fmt.Errorf("invalid value for key %s", string(key)),
		}
	}
	return binary.LittleEndian.Uint64(b), nil
}

// Get is used to get a value for a given key
func (s *MemoryStore) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, &errors.Error{
			StatusCode: errors.Closed,
			Err:        fmt.Errorf("cannot get key from closed storage"),
		}
	}
	if val, ok := s.kv[string(key)]; ok {
		return val, nil
	}
	return []byte{}, &errors.Error{
		StatusCode: errors.NotFound,
		Err:        fmt.Errorf("failed to get value for key %s", string(key)),
	}
}

// StoreLogs is used to store a set of raft logs
func (s *MemoryStore) StoreLogs(logs []*log.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return &errors.Error{
			StatusCode: errors.Closed,
			Err:        fmt.Errorf("cannot store logs in closed storage"),
		}
	}
	if s.persist {
		s.buf.clear()
		s.buf.writeByte(cmdStoreLogs)
		s.buf.writeUint64(uint64(len(logs)))
		for _, log := range logs {
			s.buf.writeLog(log)
		}
		if err := s.writeBuf(); err != nil {
			return err
		}
	}
	for _, log := range logs {
		s.logDB[log.Index] = log
		s.updateFirstAndLastIndex(log)
	}
	return nil
}

func (s *MemoryStore) updateFirstAndLastIndex(log *log.LogEntry) {
	if s.firstIndex == 0 {
		s.firstIndex, s.lastIndex = log.Index, log.Index
	} else if log.Index < s.firstIndex {
		s.firstIndex = log.Index
	} else if log.Index > s.lastIndex {
		s.lastIndex = log.Index
	}
}

func (s *MemoryStore) fillFirstAndLastIndex() {
	s.firstIndex, s.lastIndex = 0, 0
	for _, log := range s.logDB {
		if s.firstIndex == 0 {
			s.firstIndex, s.lastIndex = log.Index, log.Index
		} else if log.Index < s.firstIndex {
			s.firstIndex = log.Index
		} else if log.Index > s.lastIndex {
			s.lastIndex = log.Index
		}
	}
}

// Shrink is used to shrink the storage
// it is used to shrink the storage when the size of the file is greater than minShrinkSize
// The storage is shrunk by creating a new file and copying only the logs that are in memory
// to the new file. The new file is then renamed to the old file.
func (s *MemoryStore) Shrink() error {
	s.mu.Lock()
	if !s.persist {
		s.mu.Unlock()
		return nil
	}
	if s.shrinkMark {
		s.mu.Unlock()
		return &errors.Error{
			StatusCode: errors.Conflict,
			Err:        fmt.Errorf("failed to shrink storage: already cycling"),
		}
	}
	s.shrinkMark = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.shrinkMark = false
		s.mu.Unlock()
	}()
	return s.shrink()
}

func (s *MemoryStore) shrink() (err error) {
	start := time.Now()
	defer func() {
		if err != nil {
			s.logger.Error("shrink failed %s", slog.String("duration", time.Since(start).String()))
			return
		}
		s.logger.Info("shrink completed %s", slog.String("duration", time.Since(start).String()))
	}()

	// read logs and create a new file
	var buf buffer
	s.mu.RLock()
	// record current position of the file, so when we release the lock
	// and start reading from the file, we know where to start reading from
	pos := s.size
	// write the key value map to the buffer
	for k, v := range s.kv {
		buf.writeKV([]byte(k), v)
	}
	indexes := make([]uint64, len(s.logDB))
	for idx := range s.logDB {
		indexes = append(indexes, idx)
	}
	s.mu.RUnlock()

	newFile, err := os.Create(s.path + ".shrink")
	if err != nil {
		errf := os.Remove(s.path + ".shrink")
		return gerr.Join(err, errf)
	}

	s.mu.RLock()
	buffered := 0
	for _, idx := range indexes {
		if log, ok := s.logDB[idx]; ok {
			buf.writeByte(cmdStoreLogs)
			buf.writeUint64(uint64(1))
			buf.writeLog(log)
			buffered++
			if len(buf) > minShrinkSize || buffered >= 1000 {
				s.mu.RUnlock()
				if _, err := newFile.Write(buf); err != nil {
					errf := os.Remove(s.path + ".shrink")
					return gerr.Join(err, errf)
				}
				buf.clear()
				buffered = 0
				s.mu.RLock()
			}
		}
	}
	s.mu.RUnlock()

	if len(buf) > 0 {
		if _, err := newFile.Write(buf); err != nil {
			errf := os.Remove(s.path + ".shrink")
			return gerr.Join(err, errf)
		}
		buf.clear()
	}

	err = s.copyTo(pos, newFile)
	if err != nil {
		errf := os.Remove(s.path + ".shrink")
		return gerr.Join(err, errf)
	}

	// rename the new file to the old file
	err = newFile.Close()
	if err != nil {
		errf := os.Remove(s.path + ".shrink")
		return gerr.Join(err, errf)
	}

	err = s.file.Close()
	if err != nil {
		errf := os.Remove(s.path + ".shrink")
		return gerr.Join(err, errf)
	}

	if err := os.Rename(s.path+".shrink", s.path); err != nil {
		panic("shrink failed: " + err.Error())
	}
	s.file, err = os.OpenFile(s.path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		panic("shrink failed: " + err.Error())
	}
	size, err := s.file.Seek(0, 2)
	if err != nil {
		panic("shrink failed: " + err.Error())
	}
	s.size = int(size)
	return nil
}

// copyTo copies the tail of the database to the file
func (s *MemoryStore) copyTo(pos int, file *os.File) error {
	// write the tail of the database
	// this is a two run process, first run will read as much as possible
	// without a lock. this allows for sets and get to continue.
	// the second run will lock and finish reading any remaining.

	// run 1
	of, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer of.Close()
	// we are in persist mode, so everything we read from the file is new data that
	// has been added during the shrink process.
	if _, err := of.Seek(int64(pos), 0); err != nil {
		return err
	}
	copied, err := io.Copy(file, of)
	if err != nil {
		return err
	}
	// run 2
	// now lock and copy the rest
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := of.Seek(int64(pos+int(copied)), 0); err != nil {
		return err
	}
	if _, err := io.Copy(file, of); err != nil {
		return err
	}

	return nil
}

func (s *MemoryStore) DeleteRange(min, max uint64) (err error) {
	start := time.Now()
	defer func() {
		if err != nil {
			s.logger.Error("delete range failed %s",
				slog.Int64("min", int64(min)),
				slog.Int64("max", int64(max)),
				slog.String("duration", time.Since(start).String()))
			return
		}
		s.logger.Info("delete range completed %s",
			slog.Int64("min", int64(min)),
			slog.Int64("max", int64(max)),
			slog.String("duration", time.Since(start).String()))
	}()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return &errors.Error{
			StatusCode: errors.Closed,
			Err:        fmt.Errorf("failed to delete range from closed storage"),
		}
	}
	if s.persist {
		s.buf.clear()
		s.buf.writeByte(cmdDeleteRange)
		s.buf.writeUint64(min)
		s.buf.writeUint64(max)
		if err := s.writeBuf(); err != nil {
			return err
		}
	}

	for i := min; i <= max; i++ {
		delete(s.logDB, i)
	}
	s.fillFirstAndLastIndex()
	return nil
}
