package sio

//#cgo LDFLAGS: -luring
//#include <liburing.h>
//#include <sys/types.h>
import "C"
import (
	"os"
	"runtime"
	"runtime/cgo"
	"strconv"
	"syscall"
	"time"
	"unsafe"
)

type IOTask struct {
	Fd   int
	Op   uint8 //1 写入 2 读取
	Off  uint64
	Buff []byte
	Cb   func(int, error)
}

// 每次io合并的最大数量
const ioTaskListCap = 36 * 3

// 最大线程数量
const ioPoolNumber = 24

const (
	OP_WRITE = 1
	OP_READ  = 2
)

var ioDaemonChannels [ioPoolNumber]chan<- IOTask

func init() {
	for i := 0; i < ioPoolNumber; i++ {
		ch := make(chan IOTask, ioTaskListCap*2)
		ioDaemonChannels[i] = ch
		go RunIODaemon(ch)
	}
}
func RunIODaemon(taskChan <-chan IOTask) {
	runtime.LockOSThread()
	var (
		iq   C.struct_io_uring
		cqes [ioTaskListCap]*C.struct_io_uring_cqe
	)
	ret := C.io_uring_queue_init(C.uint(ioTaskListCap), &iq, 0)
	if ret < 0 {
		panic("io_uring_queue init failed " + syscall.Errno(uintptr(-ret)).Error())
	}
	var (
		taskList      [ioTaskListCap]*IOTask
		taskListIndex int
	)
	for {
		ti := time.NewTimer(time.Millisecond * 50)
		var need bool
		select {
		case t := <-taskChan:
			taskList[taskListIndex] = &t
			taskListIndex++
			need = taskListIndex >= ioTaskListCap
		case <-ti.C:
			need = true && taskListIndex > 0
		}
		if !need {
			continue
		}
		executeIoTasks(&iq, taskList[:taskListIndex], cqes[:])
		taskListIndex = 0
	}
}

func executeIoTasks(ring *C.struct_io_uring, tasks []*IOTask, cqes []*C.struct_io_uring_cqe) {
	for _, t := range tasks {
		sqe := C.io_uring_get_sqe(ring)
		switch t.Op {
		case OP_WRITE:
			C.io_uring_prep_write(sqe, C.int(t.Fd), unsafe.Pointer(&t.Buff[0]), C.uint(len(t.Buff)), C.__u64(t.Off))
		case OP_READ:
			C.io_uring_prep_read(sqe, C.int(t.Fd), unsafe.Pointer(&t.Buff[0]), C.uint(len(t.Buff)), C.__u64(t.Off))

		default:
			panic("unknown task op " + strconv.Itoa(int(t.Op)))
		}
		handle := cgo.NewHandle(t.Cb)
		C.io_uring_sqe_set_data(sqe, unsafe.Pointer(handle))
	}
	ret := C.io_uring_submit_and_wait(ring, C.uint(len(tasks)))
	if ret < 0 {
		panic("io uring submit and wait return error " + syscall.Errno(uintptr(-ret)).Error())
	}
	n := C.io_uring_peek_batch_cqe(ring, (**C.struct_io_uring_cqe)(unsafe.Pointer(&cqes[0])), ioTaskListCap)
	var (
		i C.uint = 0
	)
	for ; i < n; i++ {
		e := cqes[i]
		data := cgo.Handle(C.io_uring_cqe_get_data(e)).Value()
		var (
			_err error
			r    int
		)
		if e.res < 0 {
			_err = syscall.Errno(-e.res)
		} else {
			r = int(e.res)
		}
		data.(func(int, error))(r, _err)

		C.io_uring_cqe_seen(ring, e)
	}
}

func Open(fname string) (*File, error) {
	return OpenFile(fname, os.O_RDONLY, 0)
}
