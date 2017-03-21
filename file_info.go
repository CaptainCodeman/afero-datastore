package dfs

import (
	"os"
	"time"

	"path/filepath"
)

// FileInfo implements os.FileInfo for a file in a datastore filesystem
type FileInfo struct {
	fileData *FileData
}

// implements os.FileInfo
var _ os.FileInfo = (*FileInfo)(nil)

// NewFileInfo creates a new FileInfo object
func NewFileInfo(fileData *FileData) *FileInfo {
	return &FileInfo{fileData: fileData}
}

// Name is the base name of the file
func (fi FileInfo) Name() string {
	return filepath.Base(fi.fileData.name)
}

// Size is the length in bytes
func (fi FileInfo) Size() int64 {
	if fi.fileData.Directory {
		return int64(42)
	}
	return int64(len(fi.fileData.Data))
}

// Mode is the file mode bits
func (fi FileInfo) Mode() os.FileMode {
	return os.FileMode(fi.fileData.Mode)
}

// ModTime is the last modification time
func (fi FileInfo) ModTime() time.Time {
	return fi.fileData.ModTime
}

// IsDir is whether the file represents a directory
func (fi FileInfo) IsDir() bool {
	return fi.fileData.Directory
}

// Sys is the underlying FileData
func (fi FileInfo) Sys() interface{} {
	return fi.fileData
}
