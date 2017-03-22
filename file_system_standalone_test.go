// +build !appengine

package dfs

import (
	"os"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/captaincodeman/afero-datastore/test"
	"github.com/spf13/afero"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

var (
	fs afero.Fs
)

func TestMain(m *testing.M) {
	// Verbose()

	client, err := datastore.NewClient(context.Background(), "blog-serve", option.WithServiceAccountFile("service-account.json"))
	if err != nil {
		panic(err)
	}

	fs = NewFileSystem(client, "", "")
	defer func() {
		fs.RemoveAll("/tmp")
	}()

	os.Exit(m.Run())
}

func TestRead0(t *testing.T) {
	test.Read0(t, fs)
}

func TestOpenFile(t *testing.T) {
	test.OpenFile(t, fs)
}

func TestCreate(t *testing.T) {
	test.Create(t, fs)
}

func TestRename(t *testing.T) {
	test.Rename(t, fs)
}

func TestRemove(t *testing.T) {
	test.Remove(t, fs)
}

func TestTruncate(t *testing.T) {
	test.Truncate(t, fs)
}

func TestSeek(t *testing.T) {
	test.Seek(t, fs)
}

func TestReadAt(t *testing.T) {
	test.ReadAt(t, fs)
}

func TestWriteAt(t *testing.T) {
	test.WriteAt(t, fs)
}

func TestReaddirnames(t *testing.T) {
	test.Readdirnames(t, fs)
}

func TestReaddirSimple(t *testing.T) {
	test.ReaddirSimple(t, fs)
}

func TestReaddirAll(t *testing.T) {
	test.ReaddirAll(t, fs)
}

func TestStatDirectory(t *testing.T) {
	test.StatDirectory(t, fs)
}

func TestStatFile(t *testing.T) {
	test.StatFile(t, fs)
}
