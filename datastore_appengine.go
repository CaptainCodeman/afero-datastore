// +build appengine

package dfs

import (
	"os"
	"sync"

	"path/filepath"

	"github.com/qedus/nds"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
)

type (
	// clientImpl struct provides an instance with the same method signatures
	// as the "cloud.google.com/go/datastore" package so more common datastore
	// code can be reused. RunInTransaction is missing as it's different
	clientImpl struct {
		Get         func(c context.Context, key *datastore.Key, val interface{}) error
		GetMulti    func(c context.Context, keys []*datastore.Key, vals interface{}) error
		Put         func(c context.Context, key *datastore.Key, val interface{}) (*datastore.Key, error)
		PutMulti    func(c context.Context, keys []*datastore.Key, vals interface{}) ([]*datastore.Key, error)
		Delete      func(c context.Context, key *datastore.Key) error
		DeleteMulti func(c context.Context, keys []*datastore.Key) error
	}

	// FileSystem represents an appengine
	// datastore backed filesystem session
	FileSystem struct {
		sync.RWMutex
		ctx       context.Context
		client    clientImpl
		namespace string
		kind      string
		data      map[string]*FileData
	}

	clientType byte
)

var (
	standard = clientImpl{
		Get:         datastore.Get,
		GetMulti:    datastore.GetMulti,
		Put:         datastore.Put,
		PutMulti:    datastore.PutMulti,
		Delete:      datastore.Delete,
		DeleteMulti: datastore.DeleteMulti,
	}

	memcache = clientImpl{
		Get:         nds.Get,
		GetMulti:    nds.GetMulti,
		Put:         nds.Put,
		PutMulti:    nds.PutMulti,
		Delete:      nds.Delete,
		DeleteMulti: nds.DeleteMulti,
	}
)

const (
	Standard clientType = iota
	Memcache
)

// NewFileSystem creates a new appengine datastore backed filesystem
func NewFileSystem(ctx context.Context, namespace, kind string, clientType clientType) *FileSystem {
	logger.Println("create appengine datastore filesystem", namespace)

	if kind == "" {
		kind = "file"
	}
	ctx, _ = appengine.Namespace(ctx, namespace)

	var client clientImpl
	switch clientType {
	case Standard:
		client = standard
	case Memcache:
		client = memcache
	}

	return &FileSystem{
		ctx:       ctx,
		client:    client,
		namespace: namespace,
		kind:      kind,
		data:      make(map[string]*FileData),
	}
}

func (fs *FileSystem) makeKey(name string) *datastore.Key {
	return datastore.NewKey(fs.ctx, fs.kind, name, 0, nil)
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
	q = q.Offset(offset)
	q = q.KeysOnly()
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

	err := nds.RunInTransaction(fs.ctx, func(ctx context.Context) error {
		if err := fs.client.Get(ctx, oldKey, &fileData); err != nil {
			if err == datastore.ErrNoSuchEntity {
				result = err
				return nil
			}
			return err
		}

		fileData.name = newname
		fileData.Parent = newParent

		if _, err := fs.client.Put(ctx, newKey, &fileData); err != nil {
			return err
		}

		return fs.client.Delete(ctx, oldKey)
	}, &datastore.TransactionOptions{XG: true})

	if result != nil {
		return result
	}

	return err
}

func (fs *FileSystem) removeAllDescendents(path string) error {
	q := datastore.NewQuery(fs.kind)
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

	return fs.client.DeleteMulti(fs.ctx, keys)
}
