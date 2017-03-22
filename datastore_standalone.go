// +build !appengine

package dfs

import (
	"os"
	"sync"

	"path/filepath"

	"cloud.google.com/go/datastore"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

type (
	// FileSystem represents an appengine
	// datastore backed filesystem session
	FileSystem struct {
		sync.RWMutex
		ctx       context.Context
		client    *datastore.Client
		namespace string
		kind      string
		data      map[string]*FileData
	}
)

// NewFileSystem creates a new appengine datastore backed filesystem
func NewFileSystem(client *datastore.Client, namespace, kind string) *FileSystem {
	logger.Println("create standalone datastore filesystem", namespace)
	
	if kind == "" {
		kind = "file"
	}

	return &FileSystem{
		ctx:       context.Background(),
		client:    client,
		namespace: namespace,
		kind:      kind,
		data:      make(map[string]*FileData),
	}
}

func (fs *FileSystem) makeKey(name string) *datastore.Key {
	key := datastore.NameKey(fs.kind, name, nil)
	key.Namespace = fs.namespace
	return key
}

func (fs *FileSystem) loadFileData(name string) (*FileData, error) {
	key := fs.makeKey(name)
	var fileData FileData
	if err := fs.client.Get(fs.ctx, key, &fileData); err != nil {
		if err == datastore.ErrNoSuchEntity {
			return nil, ErrFileNotFound
		}
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}

	fileData.name = name
	return &fileData, nil
}

func (fs *FileSystem) saveFileData(fileData *FileData) error {
	key := fs.makeKey(fileData.name)
	_, err := fs.client.Put(fs.ctx, key, fileData)
	return err
}

func (fs *FileSystem) saveFileDataMulti(files []*FileData) error {
	keys := make([]*datastore.Key, len(files))
	for i, file := range files {
		keys[i] = fs.makeKey(file.name)
	}
	_, err := fs.client.PutMulti(fs.ctx, keys, files)
	return err
}

func (fs *FileSystem) deleteFileData(name string) error {
	key := fs.makeKey(name)
	return fs.client.Delete(fs.ctx, key)
}

// TODO: use cursor for continuation rather than offset
// TODO: provide channel / callback func to populate?
func (fs *FileSystem) readDir(name string, offset, limit int) ([]os.FileInfo, error) {
	files := []os.FileInfo{}

	q := datastore.NewQuery(fs.kind)
	q = q.Filter("parent =", name)
	q = q.Order("__key__")
	q = q.Namespace(fs.namespace)
	q = q.Offset(offset)
	if limit > 0 {
		q = q.Limit(limit)
	}

	it := fs.client.Run(fs.ctx, q)
	for {
		var fileData FileData
		k, err := it.Next(&fileData)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		fileData.name = k.Name

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

	_, err := fs.client.RunInTransaction(fs.ctx, func(tx *datastore.Transaction) error {
		if err := tx.Get(oldKey, &fileData); err != nil {
			if err == datastore.ErrNoSuchEntity {
				result = err
				return nil
			}
			return err
		}

		fileData.name = newname
		fileData.Parent = newParent

		if _, err := tx.Put(newKey, &fileData); err != nil {
			return err
		}

		return tx.Delete(oldKey)
	})

	if result != nil {
		return result
	}

	return err
}

func (fs *FileSystem) removeAllDescendents(path string) error {
	q := datastore.NewQuery(fs.kind)
	q = q.Filter("parent >=", path)
	q = q.Filter("parent <", path+"\x7F")
	q = q.Namespace(fs.namespace)
	q = q.KeysOnly()

	keys, err := fs.client.GetAll(fs.ctx, q, nil)
	if err != nil {
		return err
	}

	// add the parent
	key := fs.makeKey(path)
	keys = append(keys, key)

	return fs.client.DeleteMulti(fs.ctx, keys)
}
