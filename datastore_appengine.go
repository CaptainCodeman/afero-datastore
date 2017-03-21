// +build appengine

package dfs

import (
	"os"
	"sync"

	"path/filepath"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
)

type (
	// FileSystem represents an appengine
	// datastore backed filesystem session
	FileSystem struct {
		sync.RWMutex
		ctx       context.Context
		namespace string
		data      map[string]*FileData
	}
)

// NewFileSystem creates a new appengine datastore backed filesystem
func NewFileSystem(ctx context.Context, namespace string) *FileSystem {
	logger.Println("create appengine datastore filesystem", namespace)
	ctx, _ = appengine.Namespace(ctx, namespace)
	return &FileSystem{
		ctx:       ctx,
		namespace: namespace,
		data:      make(map[string]*FileData),
	}
}

func (fs *FileSystem) makeKey(name string) *datastore.Key {
	return datastore.NewKey(fs.ctx, fileKind, name, 0, nil)
}

func (fs *FileSystem) loadFileData(name string) (*FileData, error) {
	key := fs.makeKey(name)
	fileData := new(FileData)
	if err := datastore.Get(fs.ctx, key, fileData); err != nil {
		if err == datastore.ErrNoSuchEntity {
			return nil, ErrFileNotFound
		}
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}

	fileData.name = name
	return fileData, nil
}

func (fs *FileSystem) saveFileData(fileData *FileData) error {
	key := fs.makeKey(fileData.name)
	_, err := datastore.Put(fs.ctx, key, fileData)
	return err
}

func (fs *FileSystem) saveFileDataMulti(files []*FileData) error {
	keys := make([]*datastore.Key, len(files))
	for i, file := range files {
		keys[i] = fs.makeKey(file.name)
	}
	_, err := datastore.PutMulti(fs.ctx, keys, files)
	return err
}

func (fs *FileSystem) deleteFileData(name string) error {
	key := fs.makeKey(name)
	return datastore.Delete(fs.ctx, key)
}

// TODO: use cursor for continuation rather than offset
// TODO: provide channel / callback func to populate?
func (fs *FileSystem) readDir(name string, offset, limit int) ([]os.FileInfo, error) {
	files := []os.FileInfo{}

	q := datastore.NewQuery("file")
	q = q.Filter("parent =", name)
	q = q.Order("__key__")
	q = q.Offset(offset)
	if limit > 0 {
		q = q.Limit(limit)
	}

	it := q.Run(fs.ctx)
	for {
		var fileData FileData
		k, err := it.Next(&fileData)
		if err == datastore.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		if err := datastore.Get(fs.ctx, k, &fileData); err != nil {
			return nil, err
		}
		fileData.name = k.StringID()

		files = append(files, NewFileInfo(&fileData))
	}

	return files, nil
}

func (fs *FileSystem) rename(oldname, newname string) error {
	oldKey := fs.makeKey(oldname)
	newKey := fs.makeKey(newname)
	newParent := filepath.Dir(newname)

	var fileData FileData
	var result error

	err := datastore.RunInTransaction(fs.ctx, func(ctx context.Context) error {
		if err := datastore.Get(ctx, oldKey, &fileData); err != nil {
			if err == datastore.ErrNoSuchEntity {
				result = err
				return nil
			}
			return err
		}

		fileData.name = newname
		fileData.Parent = newParent

		if _, err := datastore.Put(ctx, newKey, &fileData); err != nil {
			return err
		}

		return datastore.Delete(ctx, oldKey)
	}, &datastore.TransactionOptions{XG: true})

	if result != nil {
		return result
	}

	return err
}

func (fs *FileSystem) removeAllDescendents(path string) error {
	q := datastore.NewQuery("file")
	q = q.Filter("parent >=", path)
	q = q.Filter("parent <", path+"\x7F")
	q = q.KeysOnly()

	keys, err := q.GetAll(fs.ctx, nil)
	if err != nil {
		return err
	}

	// add the parent
	key := fs.makeKey(path)
	keys = append(keys, key)

	return datastore.DeleteMulti(fs.ctx, keys)
}
