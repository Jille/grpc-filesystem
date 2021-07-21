# gRPC filesystem

[![GoDoc](https://godoc.org/github.com/Jille/grpc-filesystem?status.svg)](https://godoc.org/github.com/Jille/grpc-filesystem)

This library provides a gRPC interface for a remote filesystem.

There is a server library to serve the filesystem with two hooks: a hook to verify the peer's credentials, and a hook to rewrite the virtual path to a local path.

There is a client library that implements the fs.FS interface and more.

## Unsupported features

Patches are very welcome :)

* All file types except normal files and directories (symlinks, fifos, unix sockets)
* Hardlinks
* xattrs
* chown
* chmod
