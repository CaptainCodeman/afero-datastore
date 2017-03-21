package dfs

import (
	"os"
	"sync"
	"time"

	"path/filepath"
)

type (
	// FileData represents the datastore file datastore
	// key name is the filename
	FileData struct {
		// used if two file handles have the same file loaded
		sync.Mutex

		// identity of this entity
		name string

		// Mode is the filemode / permission flags
		Mode int64 `datastore:"mode,noindex"`

		// Directory is true if this file represents a directory
		Directory bool `datastore:"dir,noindex"`

		// Parent is the directory that this file is in
		Parent string `datastore:"parent"`

		// Format of Data (to support compression, encryption etc...)
		Format string `datastore:"format"`

		// Size in bytes of the data
		Size int64 `datastore:"size"`

		// Data in Format specified
		Data []byte `datastore:"data,noindex"`

		// File is the GCS file, used to store the Data if the file
		// is too large to be stored directly in datastore (1Mb limit)
		// File string `datastore:"file"`

		// ModTime is the last modification time
		ModTime time.Time `datastore:"mod_time"`
	}
)

// CreateFile creates a new file
func CreateFile(name string) *FileData {
	return &FileData{
		name:    name,
		Parent:  filepath.Dir(name),
		Mode:    int64(os.ModeTemporary),
		ModTime: time.Now(),
		Data:    make([]byte, 0),
	}
}

// CreateDir creates a new directory
func CreateDir(name string) *FileData {
	return &FileData{
		name:      name,
		Parent:    filepath.Dir(name),
		Mode:      int64(os.ModeDir),
		ModTime:   time.Now(),
		Data:      make([]byte, 0),
		Directory: true,
	}
}
