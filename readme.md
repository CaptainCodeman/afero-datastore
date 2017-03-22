# Afero Datastore FileSystem

_work in progress_

AppEngine Datastore filesystem implementation for [Afero](https://github.com/spf13/afero)

Allows the datastore to be treated as a filesystem by Go code which would, for instance, allow [Hugo](https://github.com/spf13/hugo) blog engine to run in the cloud - add a UI to edit the source files and regenerate the site when done, Afero also provides an Http filesystem which can be used to serve it.

Build flags are used to allow it to work with the `google.golang.org/appengine/datastore` package (for AppEngine Standard) or remotely via the newer `cloud.google.com/go/datastore` package (for AppEngine Flex, GCE or standalone.

## Usage

Depending on whether you are using AppEngine Standard or Flex / GCS or some other platform, the constructor will vary (this is due to there being different datastore clients for the different platforms).

### AppEngine Standard

Call `NewFileSystem` passing a `context.Context` sourced from the request (`appengine.NewContext(r)`) the namespace and kind to use for datastore entities and either `dfs.Standard` to use raw datastore calls or `dfs.Memcache` to use memcache. Note that due to AppEngine Standard restrictions, a filedsystem instance is only valid while associated with it's request.

```go
fs := dfs.NewFileSystem(ctx, "captaincodeman", "drafts", dfs.Memcache)
```

### AppEngine Flex / GCE

Create a datastore client with the `google.golang.org/appengine/datastore` package and pass to the `NewFileSystem` method along with a `context.Context` and the namespace and kind to use. Example using the default namespace and
file entities stored as `file` in the datastore:

```go
ctx := context.Background()
serviceAccount := option.WithServiceAccountFile("service-account.json")
client, _ := datastore.NewClient(ctx, "app-name", serviceAccount)

fs = NewFileSystem(client, "captaincodeman", "drafts")
```

## Notes

The namespacing feature of datastore can be used in a similar way to having separate volumes.

Currently files are only saved to the datastore so they are limited to just under 1Mb  each (usually plenty for a blog). in future this could be enhanced to use Google Cloud Storage for larger files.

To avoid too many datastore writes, the datastore entities are only written on close. Multiple filesystem sessions will therefore see inconsistent results. Access to files within the same fileysystem session will use the same file references for consistency with the same approach used as per the Afero memory file system (locks).

## Testing

Test standalone version:

    go test -v

Test appengine version:

    goapp test -v

## Known Issues

Afero isn't (yet) compatible with AppEngine Standard due to direct import of "syscall" but I have a fork that hides these behind build flags so that it can be used on the platform.

Some of the operations currently don't keep the session cache of files updated (but the tests pass and publishing via Hugo runs fine). Also, closed files should be removed to avoid excessive memory use (more critical if running on the low-memory AppEngine frontend instances).

Datastore is eventually consistent so some operations may not be immediately visible. Files are written on Close, so Directory operations will not always show newly created files (it typically works fine with Hugo).

## Enhancements

Improve usefulness of logging
Add storage-level compression (i.e. transparent to the caller)