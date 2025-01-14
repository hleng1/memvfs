package memvfs

import (
	"bytes"
	"errors"
	"io"
	"sync"

	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"
)

type MemVFS struct {
	mu    sync.Mutex
	files map[string]*bytes.Buffer
}

type MemFile struct {
	store     *MemVFS
	fileName  string
	lockLevel sqlite3vfs.LockType
	mu        sync.Mutex
}

func New() *MemVFS {
	return &MemVFS{
		files: make(map[string]*bytes.Buffer),
	}
}

// getFile returns an existing buffer for the given fileName
// or creates a new buffer if it doesn’t exist yet.
func (v *MemVFS) getFile(fileName string) *bytes.Buffer {
	v.mu.Lock()
	defer v.mu.Unlock()
	buf, ok := v.files[fileName]
	if !ok {
		buf = new(bytes.Buffer)
		v.files[fileName] = buf
	}
	return buf
}

func (v *MemVFS) GetFile(fileName string) ([]byte, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	buf, ok := v.files[fileName]
	if !ok {
		return nil, errors.New("file not found in memvfs")
	}

	return buf.Bytes(), nil
}

func (f *MemFile) ReadAt(p []byte, off int64) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	buf := f.store.getFile(f.fileName)
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

func (f *MemFile) WriteAt(p []byte, off int64) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	fileBuf := f.store.getFile(f.fileName)

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

func (f *MemFile) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	buf := f.store.getFile(f.fileName)
	currentLen := int64(buf.Len())
	if size < currentLen {
		buf.Truncate(int(size))
	} else if size > currentLen {
		padding := make([]byte, size-currentLen)
		buf.Write(padding)
	}
	return nil
}

func (f *MemFile) Sync(flags sqlite3vfs.SyncType) error {
	return nil
}

func (f *MemFile) FileSize() (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	buf := f.store.getFile(f.fileName)
	return int64(buf.Len()), nil
}

func (f *MemFile) Lock(lockType sqlite3vfs.LockType) error {
	if f.lockLevel < lockType {
		f.lockLevel = lockType
	}
	return nil
}

func (f *MemFile) Unlock(lockType sqlite3vfs.LockType) error {
	f.lockLevel = lockType
	return nil
}

func (f *MemFile) CheckReservedLock() (bool, error) {
	return f.lockLevel >= sqlite3vfs.LockReserved, nil
}

func (f *MemFile) SectorSize() int64 {
	return 512
}

func (f *MemFile) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic {
	return 0
}

// Close guarantees that the buffer is freed on db.Close() in consistency with
// in-memory sqlite db behavior.
func (f *MemFile) Close() error {
	return f.store.Delete(f.fileName, true)
}

func (v *MemVFS) FullPathname(name string) string {
	return name
}

func (v *MemVFS) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	return &MemFile{
		store:    v,
		fileName: name,
	}, flags, nil
}

func (v *MemVFS) Delete(name string, syncDir bool) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if _, ok := v.files[name]; ok {
		delete(v.files, name)
		return nil
	}

	return nil
}

// Access tests for access permission. Returns true if the requested permission
// is available. An error is returned only if the file's existance cannot be determined.
//
// https://github.com/psanford/sqlite3vfs/blob/24e1d98cf361/sqlite3vfscgo.go#L85C20-L87C53
// https://www.sqlite.org/c3ref/c_access_exists.html
func (v *MemVFS) Access(name string, flag sqlite3vfs.AccessFlag) (bool, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	_, ok := v.files[name]

	return ok, nil
}

func (v *MemVFS) FullPathName(name string) (string, error) {
	return name, nil
}
