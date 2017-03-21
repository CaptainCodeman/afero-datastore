// +build appengine

package dfs

import (
	"os"
	"testing"

	"github.com/captaincodeman/afero-datastore/test"
	"github.com/spf13/afero"
	"google.golang.org/appengine/aetest"
)

var (
	fs afero.Fs
)

func TestMain(m *testing.M) {
	// Verbose()

	ctx, done, err := aetest.NewContext()
	if err != nil {
		panic(err)
	}
	defer done()

	fs = NewFileSystem(ctx, "")
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
