// Package fsserver implements a gRPC server to do local file IO.
package fsserver

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	pb "github.com/Jille/grpc-filesystem/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service implements pb.RemoteFileSystemServiceServer and can be registered with a grpc.Server.
type Service struct {
	// "Unsafe" so it doesn't compile if we don't implement all the methods.
	pb.UnsafeRemoteFileSystemServiceServer

	callbacks Callbacks

	mtx             sync.Mutex
	fileDescriptors map[int64]*fileDescriptor
}

// Peer is an opaque object representing a peer.
type Peer interface{}

// Callbacks are the callbacks this library needs from callers.
type Callbacks interface {
	PeerFromContext(context.Context) (Peer, codes.Code, error)
	ResolvePath(peer Peer, path string) (string, codes.Code, error)
}

type fileDescriptor struct {
	owner    Peer
	f        *os.File
	lastUsed time.Time
}

// NewService instantiates a new Service.
func NewService(callbacks Callbacks) *Service {
	return &Service{
		callbacks:       callbacks,
		fileDescriptors: map[int64]*fileDescriptor{},
	}
}

// RegisterService creates the services and registers it with the given gRPC server.
func RegisterService(s grpc.ServiceRegistrar, callbacks Callbacks) {
	pb.RegisterRemoteFileSystemServiceServer(s, NewService(callbacks))
}

func (s *Service) resolvePeer(ctx context.Context) (Peer, error) {
	p, c, err := s.callbacks.PeerFromContext(ctx)
	if err != nil {
		return nil, status.Error(c, err.Error())
	}
	return p, nil
}

func (s *Service) resolveFileDescriptor(ctx context.Context, n int64) (*os.File, error) {
	p, err := s.resolvePeer(ctx)
	if err != nil {
		return nil, err
	}
	s.mtx.Lock()
	fd, ok := s.fileDescriptors[n]
	if !ok {
		s.mtx.Unlock()
		return nil, status.Errorf(codes.InvalidArgument, "invalid file descriptor %d", n)
	}
	fd.lastUsed = time.Now()
	s.mtx.Unlock()
	if fd.owner != p {
		return nil, status.Errorf(codes.InvalidArgument, "file descriptor %d is not yours", n)
	}
	return fd.f, nil
}

func (s *Service) resolvePath(ctx context.Context, fn string) (string, error) {
	p, err := s.resolvePeer(ctx)
	if err != nil {
		return "", err
	}
	return s.resolvePathWithPeer(ctx, p, fn)
}

func (s *Service) resolvePathWithPeer(ctx context.Context, peer Peer, fn string) (string, error) {
	if strings.Contains(fn, "../") {
		return "", status.Error(codes.InvalidArgument, "refusing path containing ../")
	}
	p, c, err := s.callbacks.ResolvePath(peer, fn)
	if err != nil {
		return "", status.Error(c, err.Error())
	}
	return p, nil
}

func (s *Service) osToGRPCError(err error) error {
	if errors.Is(err, os.ErrNotExist) {
		return status.Error(codes.NotFound, "ENOENT")
	}
	if errors.Is(err, os.ErrExist) {
		return status.Error(codes.AlreadyExists, "EEXIST")
	}
	if errors.Is(err, os.ErrPermission) {
		return status.Error(codes.PermissionDenied, "EPERM")
	}
	if errors.Is(err, os.ErrInvalid) {
		return status.Error(codes.InvalidArgument, "EINVAL")
	}
	if pe, ok := err.(*os.PathError); ok {
		err = pe.Err
	}
	return status.Errorf(codes.Unknown, "unknown error %q", err.Error())
}

func (s *Service) registerFileDescriptor(ctx context.Context, fh *os.File, peer Peer) int64 {
	fd := &fileDescriptor{
		owner:    peer,
		f:        fh,
		lastUsed: time.Now(),
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	for {
		n := rand.Int63()
		if _, exists := s.fileDescriptors[n]; exists {
			continue
		}
		s.fileDescriptors[n] = fd
		return n
	}
}

func (s *Service) Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	p, err := s.resolvePeer(ctx)
	if err != nil {
		return nil, err
	}
	fn, err := s.resolvePathWithPeer(ctx, p, req.GetPath())
	if err != nil {
		return nil, err
	}
	flags := 0
	if req.GetForReading() && req.GetForWriting() {
		flags |= os.O_RDWR
	} else if req.GetForReading() {
		flags |= os.O_RDONLY
	} else if req.GetForWriting() {
		flags |= os.O_WRONLY
	}
	if req.GetAppend() {
		flags |= os.O_APPEND
	}
	if req.GetAllowCreation() {
		flags |= os.O_CREATE
	}
	if req.GetExclusive() {
		flags |= os.O_EXCL
	}
	if req.GetTruncate() {
		flags |= os.O_TRUNC
	}
	if req.GetSync() {
		flags |= os.O_SYNC
	}
	var mode os.FileMode = 0777
	if req.GetCreateMode() != 0 {
		mode = os.FileMode(req.GetCreateMode() & 0777)
	}
	fh, err := os.OpenFile(fn, flags, mode)
	if err != nil {
		return nil, s.osToGRPCError(err)
	}
	fd := s.registerFileDescriptor(ctx, fh, p)
	return &pb.OpenResponse{FileDescriptor: fd}, nil
}

func (s *Service) KeepAlive(ctx context.Context, req *pb.KeepAliveRequest) (*empty.Empty, error) {
	_, err := s.resolveFileDescriptor(ctx, req.GetFileDescriptor())
	if err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}

func (s *Service) ReadAt(ctx context.Context, req *pb.ReadAtRequest) (*pb.ReadResponse, error) {
	f, err := s.resolveFileDescriptor(ctx, req.GetFileDescriptor())
	if err != nil {
		return nil, err
	}
	ret := &pb.ReadResponse{}
	buf := make([]byte, req.GetSize())
	n, err := f.ReadAt(buf, req.GetOffset())
	if err == io.EOF {
		ret.Eof = true
		err = nil
	}
	if err != nil {
		return nil, s.osToGRPCError(err)
	}
	ret.Data = buf[:n]
	return ret, nil
}

func (s *Service) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	f, err := s.resolveFileDescriptor(ctx, req.GetFileDescriptor())
	if err != nil {
		return nil, err
	}
	ret := &pb.ReadResponse{}
	buf := make([]byte, req.GetSize())
	n, err := f.Read(buf)
	if err == io.EOF {
		ret.Eof = true
		err = nil
	}
	if err != nil {
		return nil, s.osToGRPCError(err)
	}
	ret.Data = buf[:n]
	return ret, nil
}

func (s *Service) WriteAt(ctx context.Context, req *pb.WriteAtRequest) (*pb.WriteResponse, error) {
	f, err := s.resolveFileDescriptor(ctx, req.GetFileDescriptor())
	if err != nil {
		return nil, err
	}
	if _, err := f.WriteAt(req.GetData(), req.GetOffset()); err != nil {
		return nil, s.osToGRPCError(err)
	}
	return &pb.WriteResponse{}, nil
}

func (s *Service) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	f, err := s.resolveFileDescriptor(ctx, req.GetFileDescriptor())
	if err != nil {
		return nil, err
	}
	if _, err := f.Write(req.GetData()); err != nil {
		return nil, s.osToGRPCError(err)
	}
	return &pb.WriteResponse{}, nil
}

func (s *Service) Seek(ctx context.Context, req *pb.SeekRequest) (*pb.SeekResponse, error) {
	f, err := s.resolveFileDescriptor(ctx, req.GetFileDescriptor())
	if err != nil {
		return nil, err
	}
	whence := io.SeekStart
	switch req.GetWhence() {
	case pb.SeekRequest_SEEK_START:
		whence = io.SeekStart
	case pb.SeekRequest_SEEK_CURRENT:
		whence = io.SeekCurrent
	case pb.SeekRequest_SEEK_END:
		whence = io.SeekEnd
	}
	offset, err := f.Seek(req.GetOffset(), whence)
	if err != nil {
		return nil, s.osToGRPCError(err)
	}
	return &pb.SeekResponse{
		Offset: offset,
	}, nil
}

func (s *Service) Truncate(ctx context.Context, req *pb.TruncateRequest) (*empty.Empty, error) {
	fn, err := s.resolvePath(ctx, req.GetPath())
	if err != nil {
		return nil, err
	}
	return s.emptyResponse(os.Truncate(fn, req.GetSize()))
}

func (s *Service) Ftruncate(ctx context.Context, req *pb.FtruncateRequest) (*empty.Empty, error) {
	f, err := s.resolveFileDescriptor(ctx, req.GetFileDescriptor())
	if err != nil {
		return nil, err
	}
	return s.emptyResponse(f.Truncate(req.GetSize()))
}

func (s *Service) Fsync(ctx context.Context, req *pb.FsyncRequest) (*empty.Empty, error) {
	f, err := s.resolveFileDescriptor(ctx, req.GetFileDescriptor())
	if err != nil {
		return nil, err
	}
	return s.emptyResponse(f.Sync())
}

func (s *Service) Close(ctx context.Context, req *pb.CloseRequest) (*empty.Empty, error) {
	f, err := s.resolveFileDescriptor(ctx, req.GetFileDescriptor())
	if err != nil {
		return nil, err
	}
	s.mtx.Lock()
	delete(s.fileDescriptors, req.GetFileDescriptor())
	s.mtx.Unlock()
	return s.emptyResponse(f.Close())
}

func (s *Service) Rename(ctx context.Context, req *pb.RenameRequest) (*empty.Empty, error) {
	ofn, err := s.resolvePath(ctx, req.GetOldPath())
	if err != nil {
		return nil, err
	}
	nfn, err := s.resolvePath(ctx, req.GetNewPath())
	if err != nil {
		return nil, err
	}
	return s.emptyResponse(os.Rename(ofn, nfn))
}

func (s *Service) Unlink(ctx context.Context, req *pb.UnlinkRequest) (*empty.Empty, error) {
	fn, err := s.resolvePath(ctx, req.GetPath())
	if err != nil {
		return nil, err
	}
	return s.emptyResponse(os.Remove(fn))
}

func (s *Service) Mkdir(ctx context.Context, req *pb.MkdirRequest) (*empty.Empty, error) {
	fn, err := s.resolvePath(ctx, req.GetPath())
	if err != nil {
		return nil, err
	}
	var mode os.FileMode = 0777
	if req.GetMode() != 0 {
		mode = os.FileMode(req.GetMode() & 0777)
	}
	return s.emptyResponse(os.Mkdir(fn, mode))
}

func (s *Service) Rmdir(ctx context.Context, req *pb.RmdirRequest) (*empty.Empty, error) {
	fn, err := s.resolvePath(ctx, req.GetPath())
	if err != nil {
		return nil, err
	}
	return s.emptyResponse(os.Remove(fn))
}

func (s *Service) Readdir(ctx context.Context, req *pb.ReaddirRequest) (*pb.ReaddirResponse, error) {
	fn, err := s.resolvePath(ctx, req.GetPath())
	if err != nil {
		return nil, err
	}
	des, err := os.ReadDir(fn)
	if err != nil {
		return nil, s.osToGRPCError(err)
	}
	ret := &pb.ReaddirResponse{
		Entries: make([]*pb.ReaddirResponse_DirEntry, len(des)),
	}
	for i, de := range des {
		ret.Entries[i] = &pb.ReaddirResponse_DirEntry{
			Name:  de.Name(),
			IsDir: de.IsDir(),
		}
	}
	return ret, nil
}

func (s *Service) Fstat(ctx context.Context, req *pb.FstatRequest) (*pb.StatResponse, error) {
	f, err := s.resolveFileDescriptor(ctx, req.GetFileDescriptor())
	if err != nil {
		return nil, err
	}
	return s.statResponse(f.Stat())
}

func (s *Service) Stat(ctx context.Context, req *pb.StatRequest) (*pb.StatResponse, error) {
	fn, err := s.resolvePath(ctx, req.GetPath())
	if err != nil {
		return nil, err
	}
	return s.statResponse(os.Stat(fn))
}

func (s *Service) statResponse(st os.FileInfo, err error) (*pb.StatResponse, error) {
	if err != nil {
		return nil, s.osToGRPCError(err)
	}
	return &pb.StatResponse{
		Size:  st.Size(),
		Mode:  int32(st.Mode()),
		Mtime: timestamppb.New(st.ModTime()),
		IsDir: st.IsDir(),
	}, nil
}

func (s *Service) emptyResponse(err error) (*empty.Empty, error) {
	if err != nil {
		return nil, s.osToGRPCError(err)
	}
	return &empty.Empty{}, nil
}
