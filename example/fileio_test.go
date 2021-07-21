package fileio_test

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"path/filepath"
	"testing"

	"github.com/Jille/grpc-filesystem/fsclient"
	"github.com/Jille/grpc-filesystem/fsserver"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type serverCallbacks struct {
	baseDir string
}

func (serverCallbacks) PeerFromContext(ctx context.Context) (fsserver.Peer, codes.Code, error) {
	return "all peers are the same in this test", 0, nil
}

func (s serverCallbacks) ResolvePath(peer fsserver.Peer, path string) (string, codes.Code, error) {
	if peer != "all peers are the same in this test" {
		return "", codes.PermissionDenied, errors.New("invalid peer")
	}
	return filepath.Join(s.baseDir, path), 0, nil
}

func TestFileIO(t *testing.T) {
	// Set up server side.
	s := grpc.NewServer()
	fsserver.RegisterService(s, serverCallbacks{t.TempDir()})
	serverSock, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Failed to listen on random port: %v", err)
	}
	go s.Serve(serverSock)

	// Connect as a client.
	ctx := context.Background()
	addr := serverSock.Addr().(*net.TCPAddr)
	addr.IP = net.IPv4(127, 0, 0, 1)
	conn, err := grpc.Dial(addr.String(), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial server at %s: %v", addr.String(), err)
	}
	rfs := fsclient.New(ctx, conn)

	// Let's play around.
	fh, err := rfs.Create("file1.txt")
	if err != nil {
		t.Fatalf("Failed to create file1.txt: %v", err)
	}
	n, err := fh.Write([]byte("Hello world"))
	if err != nil {
		t.Errorf("Failed to write to file1.txt: %v", err)
	}
	if n != len("Hello world") {
		t.Errorf("Partial write: %v", err)
	}
	if err := fh.Close(); err != nil {
		t.Errorf("Failed to close file1.txt: %v", err)
	}
	matches, err := fs.Glob(rfs, "file*.txt")
	if err != nil {
		t.Errorf("Failed to glob: %v", err)
	}
	if diff := cmp.Diff([]string{"file1.txt"}, matches); diff != "" {
		t.Errorf("Incorrect results from glob: %s", diff)
	}
	files := 0
	if err := fs.WalkDir(rfs, "/", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			t.Errorf("Error during walkdir of %q: %v", path, err)
			return nil
		}
		if d.IsDir() {
			return nil
		}
		files++
		st, err := d.Info()
		if err != nil {
			t.Errorf("DirEntry.Info() failed: %v", err)
		}
		if st.Size() != int64(len("Hello world")) {
			t.Errorf("DirEntry.Info() returned incorrect size: want %d, got %d", len("Hello world"), st.Size())
		}
		return nil
	}); err != nil {
		t.Errorf("fs.WalkDir failed: %v", err)
	}
	if files != 1 {
		t.Errorf("fs.WalkDir found %d files; expected %d", files, 1)
	}
}
