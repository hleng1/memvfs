package memvfs

import (
	"bytes"
	"io"
	"sync"

	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"
)

type MemStore struct {
	mu    sync.Mutex
	files map[string]*bytes.Buffer
}

type BufferVFS struct {
	Name  string
	Store *MemStore
}

type BufferFile struct {
	store     *MemStore
	fileName  string
	lockLevel sqlite3vfs.LockType
	mu        sync.Mutex
}

func NewMemStore() *MemStore {
	return &MemStore{
		files: make(map[string]*bytes.Buffer),
	}
}

func (s *MemStore) getFile(fileName string) *bytes.Buffer {
	s.mu.Lock()
	defer s.mu.Unlock()
	buf, ok := s.files[fileName]
	if !ok {
		buf = new(bytes.Buffer)
		s.files[fileName] = buf
	}
	return buf
}

func (bf *BufferFile) ReadAt(p []byte, off int64) (n int, err error) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	buf := bf.store.getFile(bf.fileName)
	if off >= int64(buf.Len()) {
		return 0, io.EOF
	}

	tmp := bytes.NewReader(buf.Bytes())
	_, err = tmp.Seek(off, io.SeekStart)
	if err != nil {
		return 0, err
	}

	n, err = tmp.Read(p)
	return n, err
}

func (bf *BufferFile) WriteAt(p []byte, off int64) (n int, err error) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	fileBuf := bf.store.getFile(bf.fileName)

	currLen := int64(fileBuf.Len())
	if off > currLen {
		padding := make([]byte, off-currLen)
		fileBuf.Write(padding)
	}

	data := fileBuf.Bytes()
	newData := make([]byte, off+int64(len(p)))
	copy(newData, data)
	copy(newData[off:], p)
	fileBuf.Reset()
	fileBuf.Write(newData)

	return len(p), nil
}

func (bf *BufferFile) Truncate(size int64) error {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	buf := bf.store.getFile(bf.fileName)
	currentLen := int64(buf.Len())
	if size < currentLen {
		buf.Truncate(int(size))
	} else if size > currentLen {
		padding := make([]byte, size-currentLen)
		buf.Write(padding)
	}
	return nil
}

func (bf *BufferFile) Sync(flags sqlite3vfs.SyncType) error {
	return nil
}

func (bf *BufferFile) FileSize() (int64, error) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	buf := bf.store.getFile(bf.fileName)
	return int64(buf.Len()), nil
}

// TODO
func (bf *BufferFile) Lock(lockType sqlite3vfs.LockType) error {
	if bf.lockLevel < lockType {
		bf.lockLevel = lockType
	}
	return nil
}

// TODO
func (bf *BufferFile) Unlock(lockType sqlite3vfs.LockType) error {
	return nil
}

// TODO
func (bf *BufferFile) CheckReservedLock() (bool, error) {
	return bf.lockLevel >= sqlite3vfs.LockReserved, nil
}

func (bf *BufferFile) SectorSize() int64 {
	return 512
}

func (bf *BufferFile) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic {
	return 0
}

func (bf *BufferFile) Close() error {
	return nil
}

func (v *BufferVFS) FullPathname(name string) string {
	return name
}

func (v *BufferVFS) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	return &BufferFile{
		store:    v.Store,
		fileName: name,
	}, flags, nil
}

func (v *BufferVFS) Delete(name string, syncDir bool) error {
	v.Store.mu.Lock()
	defer v.Store.mu.Unlock()
	if _, ok := v.Store.files[name]; ok {
		delete(v.Store.files, name)
		return nil
	}

	return nil
}

func (v *BufferVFS) Access(name string, flag sqlite3vfs.AccessFlag) (bool, error) {
	v.Store.mu.Lock()
	defer v.Store.mu.Unlock()
	_, ok := v.Store.files[name]

	return ok, nil
}

func (v *BufferVFS) FullPathName(name string) (string, error) {
	return name, nil
}
