package sio

import (
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"sync"
	"syscall"
)

type File struct {
	l      sync.Mutex
	fd     int
	off    uint64
	name   string
	raw    *os.File
	closed bool
	flag   int
}

func OpenFile(fname string, flag int, mode os.FileMode) (*File, error) {
	fd, err := syscall.Open(fname, flag, uint32(mode))
	if err != nil {
		return nil, err
	}

	fddup, err := syscall.Dup(fd)
	if err != nil {
		syscall.Close(fd)
		return nil, fmt.Errorf("dup fd failed " + err.Error())
	}
	res := &File{
		name: fname,
		fd:   fd,
		flag: flag,
		raw:  os.NewFile(uintptr(fddup), fname),
	}
	return res, nil
}

// Close implements io.ReadWriteCloser.
func (f *File) Close() error {
	f.l.Lock()
	defer f.l.Unlock()
	if f.closed {
		return nil
	}
	syscall.Close(f.fd)
	f.raw.Close()
	f.closed = true
	return nil
}

// Read implements io.ReadWriteSeeker.
func (f *File) Read(p []byte) (n int, err error) {
	f.l.Lock()
	defer f.l.Unlock()
	index := f.fd % ioPoolNumber
	ch := ioDaemonChannels[index]
	res := make(chan int)
	ch <- IOTask{
		Fd:   f.fd,
		Op:   OP_READ,
		Off:  f.off,
		Buff: p,
		Cb: func(ret int, _err error) {
			err = _err
			res <- ret
		},
	}
	n = <-res
	f.off += uint64(n)
	close(res)
	//debug
	if err != nil {
		fmt.Fprintf(os.Stderr, "read error %v closed %v flag %v stack:"+string(debug.Stack())+"\n", err, f.closed, f.flag)
		if err == syscall.EBADFD || err == syscall.EBADF {
			_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), syscall.F_GETFD, 0)
			fmt.Fprintf(os.Stderr, "bad fd, fd is %d fd gettfd result %d\n", f.fd, errno)
		}
	}
	return
}

// seek参考 linux lseek(2)
// Seek implements io.ReadWriteSeeker.
func (f *File) Seek(offset int64, whence int) (int64, error) {
	f.l.Lock()
	defer f.l.Unlock()
	fs, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("get file stat failed " + err.Error())
	}
	fsize := fs.Size()
	switch whence {
	case io.SeekStart:
		if offset < 0 {
			return 0, fmt.Errorf("bad offset when start %d", offset)
		}
		newoff := offset
		// if newoff >= fsize {
		// 	newoff = fsize
		// }
		f.off = uint64(newoff)
	case io.SeekEnd:
		// if offset > 0 {
		// 	return 0, fmt.Errorf("bad offset when end %d", offset)
		// }
		newoff := fsize + offset
		if newoff < 0 {
			return 0, syscall.Errno(syscall.EINVAL)
		}
		f.off = uint64(newoff)
	case io.SeekCurrent:
		newoff := int64(f.off) + offset
		if newoff < 0 {
			return 0, syscall.Errno(syscall.EINVAL)
		}
		// if newoff >= fsize {
		// 	newoff = fsize
		// }
		f.off = uint64(newoff)
	default:
		return 0, fmt.Errorf("unknown whence %d", whence)
	}
	return int64(f.off), nil
}

// Write implements io.ReadWriteSeeker.
func (f *File) Write(p []byte) (n int, err error) {
	f.l.Lock()
	defer f.l.Unlock()
	index := f.fd % ioPoolNumber
	ch := ioDaemonChannels[index]
	res := make(chan int)
	ch <- IOTask{
		Fd:   f.fd,
		Op:   OP_WRITE,
		Off:  f.off,
		Buff: p,
		Cb: func(ret int, _err error) {
			err = _err
			res <- ret
		},
	}
	n = <-res
	f.off += uint64(n)
	close(res)
	//debug
	if err != nil {
		fmt.Fprintf(os.Stderr, "write error %v closed %v flag %v stack:"+string(debug.Stack())+"\n", err, f.closed, f.flag)
		if err == syscall.EBADFD || err == syscall.EBADF {
			_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), syscall.F_GETFD, 0)
			fmt.Fprintf(os.Stderr, "bad fd, fd is %d fd gettfd result %d\n", f.fd, errno)
		}
	}
	return
}

func (f *File) Stat() (os.FileInfo, error) {
	return f.raw.Stat()
}
func (f *File) Readdir(n int) ([]os.FileInfo, error) {
	return f.raw.Readdir(n)
}
func (f *File) Name() string {
	return f.name
}
func (f *File) ReadAt(b []byte, off int64) (n int, err error) {
	f.l.Lock()
	defer f.l.Unlock()
	index := f.fd % ioPoolNumber
	ch := ioDaemonChannels[index]
	res := make(chan int)
	ch <- IOTask{
		Fd:   f.fd,
		Op:   OP_READ,
		Off:  uint64(off),
		Buff: b,
		Cb: func(ret int, _err error) {
			err = _err
			res <- ret
		},
	}
	n = <-res
	close(res)
	return
}

func (f *File) Fd() uintptr {
	return uintptr(f.fd)
}

func (f *File) Truncate(n int64) (err error) {
	f.l.Lock()
	defer f.l.Unlock()
	return syscall.Ftruncate(f.fd, n)
}
func CreateTemp(s1 string, s2 string) (*File, error) {
	f, err := os.CreateTemp(s1, s2)
	if err != nil {
		return nil, err
	}
	return &File{
		raw:  f,
		fd:   int(f.Fd()),
		name: f.Name(),
	}, nil
}

func Create(name string) (*File, error) {
	return OpenFile(name, syscall.O_RDWR|syscall.O_CREAT|syscall.O_TRUNC, 0666)
}
