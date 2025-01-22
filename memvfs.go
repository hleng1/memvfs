package memvfs

import (
	"errors"
	"sync"

	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"
)

type MemVFS struct {
	mu    sync.Mutex
	files map[string][]byte
}

type MemFile struct {
	store     *MemVFS
	fileName  string
	lockLevel sqlite3vfs.LockType
	mu        sync.Mutex
}

func New() *MemVFS {
	return &MemVFS{
		files: make(map[string][]byte),
	}
}

// getFile returns an existing []byte for the given fileName
// or creates a new zero-length slice if it doesn’t exist yet.
func (v *MemVFS) getFile(fileName string) []byte {
	v.mu.Lock()
	defer v.mu.Unlock()

	data, ok := v.files[fileName]
	if !ok {
		data = []byte{}
		v.files[fileName] = data
	}
	return data
}

func (v *MemVFS) GetFile(fileName string) ([]byte, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	data, ok := v.files[fileName]
	if !ok {
		return nil, errors.New("file not found in memvfs")
	}

	return data, nil
}

func (f *MemFile) ReadAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	data := f.store.getFile(f.fileName)
	fileLen := int64(len(data))

	// If xRead() returns SQLITE_IOERR_SHORT_READ it must also fill in the
	// unread portions of the buffer with zeros. A VFS that fails to
	// zero-fill short reads might seem to work. However, failure to
	// zero-fill short reads will eventually lead to database corruption.
	//
	// https://www.sqlite.org/c3ref/io_methods.html
	if off >= fileLen {
		for i := range p {
			p[i] = 0
		}

		return len(p), sqlite3vfs.IOErrorShortRead
	}

	end := off + int64(len(p))
	if end > fileLen {
		n := copy(p, data[off:fileLen])
		for i := n; i < len(p); i++ {
			p[i] = 0
		}
		return len(p), sqlite3vfs.IOErrorShortRead
	}

	copy(p, data[off:end])
	return len(p), nil
}

func (f *MemFile) WriteAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	v := f.store
	v.mu.Lock()
	defer v.mu.Unlock()

	data := v.files[f.fileName]
	oldLen := int64(len(data))
	newEnd := off + int64(len(p))

	if newEnd < 0 {
		return 0, errors.New("negative offset + length")
	}

	if newEnd > oldLen {
		newData := make([]byte, newEnd)
		copy(newData, data)
		copy(newData[off:], p)

		v.files[f.fileName] = newData
	} else {
		copy(data[off:], p)
		v.files[f.fileName] = data
	}

	return len(p), nil
}

func (f *MemFile) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	v := f.store
	v.mu.Lock()
	defer v.mu.Unlock()

	data := v.files[f.fileName]
	currentLen := int64(len(data))

	if size < currentLen {
		v.files[f.fileName] = data[:size]
	} else if size > currentLen {
		newData := make([]byte, size)
		copy(newData, data)
		v.files[f.fileName] = newData
	}
	return nil
}

func (f *MemFile) Sync(flags sqlite3vfs.SyncType) error {
	return nil
}

func (f *MemFile) FileSize() (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	v := f.store
	v.mu.Lock()
	defer v.mu.Unlock()

	data := v.files[f.fileName]
	return int64(len(data)), nil
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

	delete(v.files, name)
	return nil
}

// Access tests for access permission. Returns true if the requested permission
// is available. An error is returned only if the file's existance cannot be
// determined.
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
