// Package fsclient provides a fs.FS compatible client for talking to a gRPC RemoteFileSystemService server.
package fsclient

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"sync"
	"time"

	pb "github.com/Jille/grpc-filesystem/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FileSystem provides an API to the remote filesystem.
type FileSystem struct {
	ctx    context.Context
	client pb.RemoteFileSystemServiceClient

	mtx               sync.Mutex
	Files             map[int64]*File
	keepAliverRunning bool
}

var _ fs.FS = &FileSystem{}
var _ fs.ReadDirFS = &FileSystem{}
var _ fs.StatFS = &FileSystem{}

// New creates a FileSystem object.
func New(ctx context.Context, c grpc.ClientConnInterface) *FileSystem {
	return &FileSystem{
		ctx:    ctx,
		client: pb.NewRemoteFileSystemServiceClient(c),
		Files:  map[int64]*File{},
	}
}

type File struct {
	parent *FileSystem
	name   string
	fd     int64
}

var _ io.Reader = &File{}
var _ io.ReaderAt = &File{}
var _ io.Writer = &File{}
var _ io.WriterAt = &File{}
var _ io.Seeker = &File{}

func translateRemoteError(err error, op, name string) error {
	pe := &fs.PathError{
		Op:   op,
		Path: name,
		Err:  err,
	}
	switch status.Code(err) {
	case codes.NotFound:
		pe.Err = fs.ErrNotExist
	case codes.InvalidArgument:
		pe.Err = fs.ErrInvalid
	case codes.AlreadyExists:
		pe.Err = fs.ErrExist
	case codes.PermissionDenied:
		pe.Err = fs.ErrPermission
	}
	return pe
}

func (h *File) translateRemoteError(err error, op string) error {
	return translateRemoteError(err, op, h.name)
}

func handleEmptyResponse(_ *empty.Empty, err error, op, name string) error {
	if err != nil {
		return translateRemoteError(err, op, name)
	}
	return nil
}

func (h *File) handleEmptyResponse(e *empty.Empty, err error, op string) error {
	return handleEmptyResponse(e, err, op, h.name)
}

func (f *FileSystem) Open(name string) (fs.File, error) {
	return f.open(&pb.OpenRequest{
		Path:       name,
		ForReading: true,
	})
}

func (f *FileSystem) Create(name string) (*File, error) {
	return f.open(&pb.OpenRequest{
		Path:          name,
		ForReading:    true,
		ForWriting:    true,
		AllowCreation: true,
		Truncate:      true,
		CreateMode:    0666,
	})
}

func (f *FileSystem) OpenFile(name string, flags int, perm fs.FileMode) (*File, error) {
	req := &pb.OpenRequest{
		Path:       name,
		CreateMode: uint32(perm),
	}
	if flags&os.O_RDWR > 0 {
		req.ForReading = true
		req.ForWriting = true
	} else if flags&os.O_RDONLY > 0 {
		req.ForReading = true
	} else if flags&os.O_WRONLY > 0 {
		req.ForWriting = true
	}
	if flags&os.O_APPEND > 0 {
		req.Append = true
	}
	if flags&os.O_CREATE > 0 {
		req.AllowCreation = true
	}
	if flags&os.O_EXCL > 0 {
		req.Exclusive = true
	}
	if flags&os.O_SYNC > 0 {
		req.Sync = true
	}
	if flags&os.O_TRUNC > 0 {
		req.Truncate = true
	}
	supported := os.O_RDWR | os.O_RDONLY | os.O_WRONLY | os.O_APPEND | os.O_CREATE | os.O_EXCL | os.O_SYNC | os.O_TRUNC
	if flags & ^supported > 0 {
		return nil, errors.New("unsupported flags given to OpenFile")
	}
	return f.open(req)
}

func (f *FileSystem) open(req *pb.OpenRequest) (*File, error) {
	resp, err := f.client.Open(f.ctx, req)
	if err != nil {
		return nil, translateRemoteError(err, "open", req.GetPath())
	}
	fh := &File{
		parent: f,
		name:   req.GetPath(),
		fd:     resp.GetFileDescriptor(),
	}
	f.mtx.Lock()
	f.Files[fh.fd] = fh
	f.maybeStartKeepAliver()
	f.mtx.Unlock()
	return fh, nil
}

func (f *FileSystem) maybeStartKeepAliver() {
	if !f.keepAliverRunning {
		f.keepAliverRunning = true
		go f.keepAliver()
	}
}

func (f *FileSystem) keepAliver() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	cancel := func() {}
	for range t.C {
		cancel()
		f.mtx.Lock()
		if len(f.Files) == 0 {
			f.keepAliverRunning = false
			f.mtx.Unlock()
			return
		}
		ids := make([]int64, 0, len(f.Files))
		for n := range f.Files {
			ids = append(ids, n)
		}
		f.mtx.Unlock()
		var ctx context.Context
		ctx, cancel = context.WithTimeout(f.ctx, time.Minute)
		for _, id := range ids {
			go f.client.KeepAlive(ctx, &pb.KeepAliveRequest{
				FileDescriptor: id,
			})
		}
	}
}

func (h *File) Close() error {
	h.parent.mtx.Lock()
	delete(h.parent.Files, h.fd)
	h.parent.mtx.Unlock()
	if _, err := h.parent.client.Close(h.parent.ctx, &pb.CloseRequest{
		FileDescriptor: h.fd,
	}); err != nil {
		return h.translateRemoteError(err, "close")
	}
	return nil
}

func (h *File) Stat() (fs.FileInfo, error) {
	resp, err := h.parent.client.Fstat(h.parent.ctx, &pb.FstatRequest{
		FileDescriptor: h.fd,
	})
	if err != nil {
		return nil, h.translateRemoteError(err, "stat")
	}
	return fileInfo{h.name, resp}, nil
}

func (f *FileSystem) Stat(name string) (fs.FileInfo, error) {
	resp, err := f.client.Stat(f.ctx, &pb.StatRequest{
		Path: name,
	})
	if err != nil {
		return nil, translateRemoteError(err, "stat", name)
	}
	return fileInfo{name, resp}, nil
}

type fileInfo struct {
	path string
	stat *pb.StatResponse
}

func (i fileInfo) Name() string {
	return i.path
}

func (i fileInfo) Size() int64 {
	return i.stat.GetSize()
}

func (i fileInfo) Mode() fs.FileMode {
	return fs.FileMode(i.stat.GetMode())
}

func (i fileInfo) ModTime() time.Time {
	return i.stat.GetMtime().AsTime()
}

func (i fileInfo) IsDir() bool {
	return i.stat.GetIsDir()
}

func (i fileInfo) Sys() interface{} {
	return i.stat
}

func (h *File) ReadAt(p []byte, offset int64) (int, error) {
	resp, err := h.parent.client.ReadAt(h.parent.ctx, &pb.ReadAtRequest{
		FileDescriptor: h.fd,
		Offset:         offset,
		Size:           int64(len(p)),
	})
	if err != nil {
		return 0, h.translateRemoteError(err, "read")
	}
	return h.handleReadResponse(p, resp)
}

func (h *File) Read(p []byte) (int, error) {
	resp, err := h.parent.client.Read(h.parent.ctx, &pb.ReadRequest{
		FileDescriptor: h.fd,
		Size:           int64(len(p)),
	})
	if err != nil {
		return 0, h.translateRemoteError(err, "read")
	}
	return h.handleReadResponse(p, resp)
}

func (h *File) handleReadResponse(p []byte, resp *pb.ReadResponse) (int, error) {
	n := copy(p, resp.GetData())
	if resp.GetEof() {
		return n, io.EOF
	}
	return n, nil
}

func (h *File) WriteAt(p []byte, offset int64) (int, error) {
	_, err := h.parent.client.WriteAt(h.parent.ctx, &pb.WriteAtRequest{
		FileDescriptor: h.fd,
		Offset:         offset,
		Data:           p,
	})
	if err != nil {
		return 0, h.translateRemoteError(err, "write")
	}
	return len(p), nil
}

func (h *File) Write(p []byte) (int, error) {
	_, err := h.parent.client.Write(h.parent.ctx, &pb.WriteRequest{
		FileDescriptor: h.fd,
		Data:           p,
	})
	if err != nil {
		return 0, h.translateRemoteError(err, "write")
	}
	return len(p), nil
}

func (h *File) Seek(offset int64, whence int) (int64, error) {
	req := &pb.SeekRequest{
		FileDescriptor: h.fd,
		Offset:         offset,
	}
	switch whence {
	case io.SeekStart:
		req.Whence = pb.SeekRequest_SEEK_START
	}
	resp, err := h.parent.client.Seek(h.parent.ctx, req)
	if err != nil {
		return 0, h.translateRemoteError(err, "seek")
	}
	return resp.GetOffset(), nil
}

func (h *File) Sync() error {
	if _, err := h.parent.client.Fsync(h.parent.ctx, &pb.FsyncRequest{
		FileDescriptor: h.fd,
	}); err != nil {
		return h.translateRemoteError(err, "sync")
	}
	return nil
}

func (h *File) Truncate(size int64) error {
	if _, err := h.parent.client.Ftruncate(h.parent.ctx, &pb.FtruncateRequest{
		FileDescriptor: h.fd,
		Size:           size,
	}); err != nil {
		return h.translateRemoteError(err, "truncate")
	}
	return nil
}

func (f *FileSystem) Truncate(name string, size int64) error {
	if _, err := f.client.Truncate(f.ctx, &pb.TruncateRequest{
		Path: name,
		Size: size,
	}); err != nil {
		return translateRemoteError(err, "truncate", name)
	}
	return nil
}

// TODO: Return *os.LinkError, not *os.PathError
func (f *FileSystem) Rename(oldName, newName string) error {
	if _, err := f.client.Rename(f.ctx, &pb.RenameRequest{
		OldPath: oldName,
		NewPath: newName,
	}); err != nil {
		return translateRemoteError(err, "rename", oldName)
	}
	return nil
}

func (f *FileSystem) ReadDir(name string) ([]fs.DirEntry, error) {
	resp, err := f.client.Readdir(f.ctx, &pb.ReaddirRequest{
		Path: name,
	})
	if err != nil {
		return nil, translateRemoteError(err, "readdir", name)
	}
	ret := make([]fs.DirEntry, len(resp.GetEntries()))
	for i, de := range resp.GetEntries() {
		ret[i] = dirEnt{
			parent:    f,
			directory: name,
			p:         de,
		}
	}
	return ret, nil
}

type dirEnt struct {
	parent    *FileSystem
	directory string
	p         *pb.ReaddirResponse_DirEntry
}

func (de dirEnt) Name() string {
	return de.p.GetName()
}

func (de dirEnt) IsDir() bool {
	return de.p.GetIsDir()
}

func (de dirEnt) Type() fs.FileMode {
	if de.IsDir() {
		return fs.ModeDir
	}
	return 0
}

func (de dirEnt) Info() (fs.FileInfo, error) {
	return de.parent.Stat(path.Join(de.directory, de.p.GetName()))
}

func (f *FileSystem) Mkdir(name string, mode fs.FileMode) error {
	if _, err := f.client.Mkdir(f.ctx, &pb.MkdirRequest{
		Path: name,
		Mode: uint32(mode),
	}); err != nil {
		return translateRemoteError(err, "mkdir", name)
	}
	return nil
}

func (f *FileSystem) Rmdir(name string) error {
	if _, err := f.client.Rmdir(f.ctx, &pb.RmdirRequest{
		Path: name,
	}); err != nil {
		return translateRemoteError(err, "rmdir", name)
	}
	return nil
}

func (f *FileSystem) Remove(name string) error {
	if _, err := f.client.Unlink(f.ctx, &pb.UnlinkRequest{
		Path: name,
	}); err != nil {
		return translateRemoteError(err, "remove", name)
	}
	return nil
}
