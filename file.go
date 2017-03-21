package dfs

import (
	"bytes"
	"errors"
	"io"
	"os"
	"time"

	"path/filepath"
	"sync/atomic"

	"github.com/spf13/afero"
)

type (
	// File represents a file handle within
	// a datastore filesystem session
	File struct {
		// atomic requires 64-bit alignment for struct field access
		at           int64
		readDirCount int64
		closed       bool
		readOnly     bool
		fileData     *FileData
		fs           *FileSystem
	}
)

// implements afero.File
var _ afero.File = (*File)(nil)

var (
	ErrFileClosed        = errors.New("File is closed")
	ErrOutOfRange        = errors.New("Out of range")
	ErrTooLarge          = errors.New("Too large")
	ErrFileNotFound      = os.ErrNotExist
	ErrFileExists        = os.ErrExist
	ErrDestinationExists = os.ErrExist
)

// NewFileHandle initializes a File object
func NewFileHandle(fs *FileSystem, fileData *FileData) *File {
	return &File{
		fs:       fs,
		fileData: fileData,
	}
}

// NewReadOnlyFileHandle initializes a File object
func NewReadOnlyFileHandle(fs *FileSystem, fileData *FileData) *File {
	return &File{
		readOnly: true,
		fs:       fs,
		fileData: fileData,
	}
}

func (f *File) Open() error {
	atomic.StoreInt64(&f.at, 0)
	atomic.StoreInt64(&f.readDirCount, 0)
	f.fileData.Lock()
	f.closed = false
	f.fileData.Unlock()
	return nil
}

func (f *File) Close() error {
	f.fileData.Lock()
	defer f.fileData.Unlock()

	f.closed = true
	f.fileData.ModTime = time.Now()
	f.fileData.Size = int64(len(f.fileData.Data))

	// TODO: write on save every time? store dirty flag on fileData? last reference?
	return f.fs.saveFileData(f.fileData)
}

// Name returns the filename
func (f *File) Name() string {
	return f.fileData.name
}

func (f *File) Stat() (os.FileInfo, error) {
	return &FileInfo{f.fileData}, nil
}

func (f *File) Sync() error {
	return nil
}

func (f *File) Readdir(count int) ([]os.FileInfo, error) {
	logger.Println("Readdir", count, f.readDirCount)
	files, err := f.fs.readDir(f.fileData.name, int(f.readDirCount), count)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 && count > 0 {
		return nil, io.EOF
	}

	f.readDirCount += int64(len(files))

	return files, nil
}

func (f *File) Readdirnames(n int) ([]string, error) {
	logger.Println("Readdirnames", n, f.readDirCount)

	fi, err := f.Readdir(n)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(fi))
	for i, f := range fi {
		_, names[i] = filepath.Split(f.Name())
	}
	return names, err
}

func (f *File) Read(data []byte) (int, error) {
	logger.Println("Read", len(data))

	f.fileData.Lock()
	defer f.fileData.Unlock()

	if f.closed {
		return 0, ErrFileClosed
	}
	if len(data) > 0 && int(f.at) == len(f.fileData.Data) {
		return 0, io.EOF
	}

	var n int
	if len(f.fileData.Data)-int(f.at) >= len(data) {
		n = len(data)
	} else {
		n = len(f.fileData.Data) - int(f.at)
	}

	copy(data, f.fileData.Data[f.at:f.at+int64(n)])
	atomic.AddInt64(&f.at, int64(n))
	return n, nil
}

func (f *File) ReadAt(data []byte, off int64) (int, error) {
	atomic.StoreInt64(&f.at, off)
	return f.Read(data)
}

func (f *File) Truncate(size int64) error {
	f.fileData.Lock()
	defer f.fileData.Unlock()

	if f.closed == true {
		return ErrFileClosed
	}
	if size < 0 {
		return ErrOutOfRange
	}
	if size > int64(len(f.fileData.Data)) {
		diff := size - int64(len(f.fileData.Data))
		f.fileData.Data = append(f.fileData.Data, bytes.Repeat([]byte{00}, int(diff))...)
	} else {
		f.fileData.Data = f.fileData.Data[0:size]
	}
	f.fileData.ModTime = time.Now()
	return nil
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	logger.Println("Seek", offset, whence)

	if f.closed {
		return 0, ErrFileClosed
	}
	switch whence {
	case 0:
		atomic.StoreInt64(&f.at, offset)
	case 1:
		atomic.AddInt64(&f.at, int64(offset))
	case 2:
		atomic.StoreInt64(&f.at, int64(len(f.fileData.Data))+offset)
	}
	return f.at, nil
}

func (f *File) Write(data []byte) (int, error) {
	logger.Println("Write", len(data))

	n := len(data)
	cur := atomic.LoadInt64(&f.at)

	f.fileData.Lock()
	defer f.fileData.Unlock()

	diff := cur - int64(len(f.fileData.Data))
	var tail []byte
	if n+int(cur) < len(f.fileData.Data) {
		tail = f.fileData.Data[n+int(cur):]
	}
	if diff > 0 {
		f.fileData.Data = append(bytes.Repeat([]byte{00}, int(diff)), data...)
		f.fileData.Data = append(f.fileData.Data, tail...)
	} else {
		f.fileData.Data = append(f.fileData.Data[:cur], data...)
		f.fileData.Data = append(f.fileData.Data, tail...)
	}
	f.fileData.ModTime = time.Now().UTC()
	atomic.StoreInt64(&f.at, int64(len(f.fileData.Data)))

	return n, nil
}

func (f *File) WriteAt(data []byte, off int64) (int, error) {
	atomic.StoreInt64(&f.at, off)
	return f.Write(data)
}

func (f *File) WriteString(s string) (ret int, err error) {
	return f.Write([]byte(s))
}
