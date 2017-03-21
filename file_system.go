package dfs

import (
	"fmt"
	"os"
	"time"

	"path/filepath"

	"github.com/spf13/afero"
)

const (
	fileKind          = "file"
	filePathSeparator = string(filepath.Separator)
)

var _ afero.Fs = (*FileSystem)(nil)

// Create creates a file in the filesystem, returning the file and an
// error, if any happens.
func (fs *FileSystem) Create(name string) (afero.File, error) {
	logger.Println("Create", name)
	name = normalizePath(name)
	path := filepath.Dir(name)

	if err := fs.MkdirAll(path, os.ModeDir); err != nil {
		return nil, &os.PathError{Op: "create", Path: name, Err: err}
	}

	fs.Lock()
	fileData := CreateFile(name)
	fs.data[name] = fileData
	fs.Unlock()

	if err := fs.saveFileData(fileData); err != nil {
		return nil, &os.PathError{Op: "create", Path: name, Err: err}
	}

	return NewFileHandle(fs, fileData), nil
}

// Mkdir creates a directory in the filesystem, return an error if any
// happens.
func (fs *FileSystem) Mkdir(name string, perm os.FileMode) error {
	logger.Println("Mkdir", name)
	clean := filepath.Clean(name)

	fs.RLock()
	fileData, ok := fs.data[clean]
	fs.RUnlock()

	if ok {
		return &os.PathError{Op: "mkdir", Path: name, Err: ErrFileExists}
	}

	fs.Lock()
	defer fs.Unlock()

	fileData = CreateDir(clean)
	fileData.Mode = int64(perm)

	if err := fs.saveFileData(fileData); err != nil {
		return &os.PathError{Op: "mkdir", Path: name, Err: err}
	}

	fs.data[clean] = fileData

	return nil
}

// MkdirAll creates a directory path and all parents that does not exist
// yet.
func (fs *FileSystem) MkdirAll(path string, perm os.FileMode) error {
	logger.Println("MkdirAll", path)
	clean := filepath.Clean(path)

	fs.Lock()
	defer fs.Unlock()

	// walk the tree up to the root until we reach a directory
	create := make([]string, 0, 8)
	var err error
	var dir *FileData
	curr := clean
	for len(curr) > 1 {
		curr = filepath.Dir(curr)
		if dir, err = fs.lockfreeOpen(curr); err == nil {
			break
		}
		if !os.IsNotExist(err) {
			return err
		}
		logger.Println("create", curr)
		create = append(create, curr)
	}

	// if we found a parent, it has to be a directory
	if dir != nil && !dir.Directory {
		logger.Println("not a dir parent")
		return ErrFileNotFound // TODO: better error
	}

	create = append(create, clean)
	count := len(create)
	files := make([]*FileData, count)
	for i := 0; i < count; i++ {
		fileData := CreateDir(create[i])
		fileData.Mode = int64(perm)

		files[i] = fileData
		fs.data[create[i]] = fileData
	}

	if err := fs.saveFileDataMulti(files); err != nil {
		return &os.PathError{Op: "mkdirall", Path: clean, Err: err}
	}
	return nil
}

// Open opens a file, returning it or an error, if any happens.
func (fs *FileSystem) Open(name string) (afero.File, error) {
	logger.Println("Open", name)

	fileData, err := fs.open(name)
	if err != nil {
		return nil, err
	}

	return NewReadOnlyFileHandle(fs, fileData), nil
}

// OpenFile opens a file using the given flags and the given mode.
func (fs *FileSystem) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	logger.Println("OpenFile", name)
	chmod := false
	file, err := fs.openWrite(name)
	if os.IsNotExist(err) && (flag&os.O_CREATE > 0) {
		file, err = fs.Create(name)
		chmod = true
	}

	if err != nil {
		return nil, err
	}

	if flag == os.O_RDONLY {
		file = NewReadOnlyFileHandle(fs, file.(*File).fileData)
	}

	if flag&os.O_APPEND > 0 {
		_, err = file.Seek(0, os.SEEK_END)
		if err != nil {
			file.Close()
			return nil, err
		}
	}
	if flag&os.O_TRUNC > 0 && flag&(os.O_RDWR|os.O_WRONLY) > 0 {
		err = file.Truncate(0)
		if err != nil {
			file.Close()
			return nil, err
		}
	}
	if chmod {
		fs.Chmod(name, perm)
	}

	return file, nil
}

// Remove removes a file identified by name, returning an error, if any
// happens.
func (fs *FileSystem) Remove(name string) error {
	logger.Println("Remove", name)
	name = normalizePath(name)

	_, err := fs.open(name)
	if err != nil {
		return err
		// &os.PathError{Op: "remove", Path: name, Err: os.ErrNotExist}
	}

	fs.Lock()
	defer fs.Unlock()

	delete(fs.data, name)

	if err := fs.deleteFileData(name); err != nil {
		return &os.PathError{Op: "remove", Path: name, Err: err}
	}

	return nil
}

// RemoveAll removes a directory path and any children it contains. It
// does not fail if the path does not exist (return nil).
func (fs *FileSystem) RemoveAll(path string) error {
	logger.Println("Remove", path)
	path = normalizePath(path)

	_, err := fs.open(path)
	if err != nil {
		return err
	}

	fs.Lock()
	defer fs.Unlock()

	return fs.removeAllDescendents(path)
}

// Rename renames a file.
func (fs *FileSystem) Rename(oldname, newname string) error {
	oldname = normalizePath(oldname)
	newname = normalizePath(newname)

	if oldname == newname {
		return nil
	}

	// TODO: lookup in memory, remove when rename completed
	return fs.rename(oldname, newname)
}

// Stat returns a FileInfo describing the named file, or an error, if any
// happens.
func (fs *FileSystem) Stat(name string) (os.FileInfo, error) {
	logger.Println("Stat", name)
	fileData, err := fs.open(name)
	if err != nil {
		return nil, err
	}

	return NewFileInfo(fileData), nil
}

// Name is the name of this FileSystem
func (fs *FileSystem) Name() string {
	return "Datastore Fs"
}

// Chmod changes the mode of the named file to mode.
func (fs *FileSystem) Chmod(name string, mode os.FileMode) error {
	logger.Println("fs Chmod")
	return fmt.Errorf("not implemented")
}

// Chtimes changes the access and modification times of the named file
func (fs *FileSystem) Chtimes(name string, atime time.Time, mtime time.Time) error {
	logger.Println("fsChtimesMkdir")
	return fmt.Errorf("not implemented")
}

func (fs *FileSystem) openWrite(name string) (afero.File, error) {
	f, err := fs.open(name)
	if err != nil {
		return nil, err
	}
	return NewFileHandle(fs, f), err
}

func (fs *FileSystem) open(name string) (*FileData, error) {
	logger.Println("open", name)
	name = normalizePath(name)

	fs.RLock()
	fileData, ok := fs.data[name]
	fs.RUnlock()

	if ok {
		return fileData, nil
	}

	fileData, err := fs.loadFileData(name)
	if err != nil {
		return nil, err
	}
	fs.data[name] = fileData
	return fileData, nil
}

func (fs *FileSystem) lockfreeOpen(name string) (*FileData, error) {
	name = normalizePath(name)
	fileData, ok := fs.data[name]

	if ok {
		return fileData, nil
	}

	fileData, err := fs.loadFileData(name)
	if err != nil {
		return nil, err
	}
	fs.data[name] = fileData
	return fileData, nil
}

func hasTrailingSlash(path string) bool {
	return len(path) > 0 && os.IsPathSeparator(path[len(path)-1])
}

func normalizePath(path string) string {
	path = filepath.Clean(path)

	switch path {
	case ".":
		return filePathSeparator
	case "..":
		return filePathSeparator
	default:
		return path
	}
}
