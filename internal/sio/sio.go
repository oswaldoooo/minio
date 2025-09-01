package sio

//#cgo LDFLAGS: -luring
//#include <liburing.h>
//#include <sys/types.h>
//#include <stdlib.h>
//#include <string.h>
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
const ioTaskListCap = 36 * 30

// 最大线程数量
const ioPoolNumber = 2

const (
	OP_WRITE     = 1
	OP_READ      = 2
	pollInterval = time.Millisecond * 10
)

var ioDaemonChannels [ioPoolNumber]chan<- IOTask

func init() {
	for i := 0; i < ioPoolNumber; i++ {
		ch := make(chan IOTask, ioTaskListCap*2)
		ioDaemonChannels[i] = ch
		go RunIODaemon2(ch)
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
		ti := time.NewTimer(pollInterval)
		var need bool
		select {
		case t := <-taskChan:
			taskList[taskListIndex] = &t
			taskListIndex++
			need = taskListIndex >= ioTaskListCap
		case <-ti.C:
			need = true && taskListIndex > 0
		}
		ti.Stop()
		if !need {
			continue
		}
		executeIoTasks(&iq, taskList[:taskListIndex], cqes[:])
		taskListIndex = 0
	}
}

type callbackArg struct {
	cb   func(int, error)
	buff []byte
	op   uint8
	cmem unsafe.Pointer
}

func executeIoTasks(ring *C.struct_io_uring, tasks []*IOTask, cqes []*C.struct_io_uring_cqe) {
	for _, t := range tasks {
		sqe := C.io_uring_get_sqe(ring)
		var arg = &callbackArg{
			buff: t.Buff,
			cb:   t.Cb,
			op:   t.Op,
		}
		bufflen := len(t.Buff)
		switch t.Op {
		case OP_WRITE:
			arg.cmem = C.malloc(C.size_t(bufflen))
			C.memmove(arg.cmem, unsafe.Pointer(&arg.buff[0]), C.size_t(bufflen))
			C.io_uring_prep_write(sqe, C.int(t.Fd), arg.cmem, C.uint(bufflen), C.__u64(t.Off))
		case OP_READ:
			arg.cmem = C.malloc(C.size_t(bufflen))
			C.io_uring_prep_read(sqe, C.int(t.Fd), arg.cmem, C.uint(bufflen), C.__u64(t.Off))
		default:
			panic("unknown task op " + strconv.Itoa(int(t.Op)))
		}
		handle := cgo.NewHandle(arg)
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
		data := cgo.Handle(C.io_uring_cqe_get_data(e))
		dat := data.Value().(*callbackArg)
		var (
			_err error
			r    int
		)
		if e.res < 0 {
			_err = syscall.Errno(-e.res)
		} else {
			r = int(e.res)
		}
		if r > 0 && dat.op == OP_READ {
			C.memmove(unsafe.Pointer(&dat.buff[0]), dat.cmem, C.size_t(e.res))
		}
		dat.cb(r, _err)
		C.free(dat.cmem)
		data.Delete()

		C.io_uring_cqe_seen(ring, e)
	}
}

func Open(fname string) (*File, error) {
	return OpenFile(fname, os.O_RDONLY, 0)
}

func RunIODaemon2(taskChan <-chan IOTask) {
	runtime.LockOSThread()
	var (
		iq      C.struct_io_uring
		cqes    [ioTaskListCap]*C.struct_io_uring_cqe
		inQueue C.uint
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
		ti := time.NewTimer(pollInterval)
		if inQueue > 0 {
			inQueue -= consumeIoTask(&iq, cqes[:])
			if inQueue >= ioTaskListCap {
				continue
			}
		}
		var need bool
		select {
		case t := <-taskChan:
			taskList[taskListIndex] = &t
			taskListIndex++
			need = taskListIndex >= ioTaskListCap
		case <-ti.C:
			need = true && taskListIndex > 0
		}
		ti.Stop()
		if !need {
			continue
		}
		n := executeIoTasks2(&iq, taskList[:taskListIndex])
		if n > 0 {
			inQueue += C.uint(n)
		}
		taskListIndex = 0
	}

}
func peekCqe(ring *C.struct_io_uring, cqes []*C.struct_io_uring_cqe) C.uint {
	n := C.io_uring_peek_batch_cqe(ring, (**C.struct_io_uring_cqe)(unsafe.Pointer(&cqes[0])), C.uint(len(cqes)))
	var i C.uint
	for ; i < n; i++ {
		e := cqes[i]
		data := cgo.Handle(C.io_uring_cqe_get_data(e))
		dat := data.Value().(*callbackArg)
		var (
			_err error
			r    int
		)
		if e.res < 0 {
			_err = syscall.Errno(-e.res)
		} else {
			r = int(e.res)
		}
		if r > 0 && dat.op == OP_READ {
			C.memmove(unsafe.Pointer(&dat.buff[0]), dat.cmem, C.size_t(e.res))
		}
		dat.cb(r, _err)
		C.free(dat.cmem)
		data.Delete()
		C.io_uring_cqe_seen(ring, e)
		cqes[i] = nil
	}
	return n
}
func consumeIoTask(ring *C.struct_io_uring, cqes []*C.struct_io_uring_cqe) C.uint {
	n := C.io_uring_cq_ready(ring)
	cqelen := C.uint(len(cqes))
	for n >= cqelen {
		n -= peekCqe(ring, cqes)
	}
	if n == 0 {
		return 0
	} else {
		peekCqe(ring, cqes)
	}
	return n
}
func executeIoTasks2(ring *C.struct_io_uring, tasks []*IOTask) int {
	for i, t := range tasks {
		sqe := C.io_uring_get_sqe(ring)
		var arg = &callbackArg{
			buff: t.Buff,
			cb:   t.Cb,
			op:   t.Op,
		}
		bufflen := len(t.Buff)
		switch t.Op {
		case OP_WRITE:
			arg.cmem = C.malloc(C.size_t(bufflen))
			C.memmove(arg.cmem, unsafe.Pointer(&arg.buff[0]), C.size_t(bufflen))
			C.io_uring_prep_write(sqe, C.int(t.Fd), arg.cmem, C.uint(bufflen), C.__u64(t.Off))
		case OP_READ:
			arg.cmem = C.malloc(C.size_t(bufflen))
			C.io_uring_prep_read(sqe, C.int(t.Fd), arg.cmem, C.uint(bufflen), C.__u64(t.Off))
		default:
			panic("unknown task op " + strconv.Itoa(int(t.Op)))
		}
		handle := cgo.NewHandle(arg)
		C.io_uring_sqe_set_data(sqe, unsafe.Pointer(handle))
		tasks[i] = nil
	}
	ret := C.io_uring_submit(ring)
	if ret < 0 {
		panic("iouring submit failed")
	}
	return int(ret)
	// ret := C.io_uring_submit_and_wait_timeout(ring, C.uint(len(tasks)))
	// if ret < 0 {
	// 	panic("io uring submit and wait return error " + syscall.Errno(uintptr(-ret)).Error())
	// }
	// n := C.io_uring_peek_batch_cqe(ring, (**C.struct_io_uring_cqe)(unsafe.Pointer(&cqes[0])), ioTaskListCap)
	// var (
	// 	i C.uint = 0
	// )
	// for ; i < n; i++ {
	// 	e := cqes[i]
	// 	data := cgo.Handle(C.io_uring_cqe_get_data(e))
	// 	dat := data.Value().(*callbackArg)
	// 	var (
	// 		_err error
	// 		r    int
	// 	)
	// 	if e.res < 0 {
	// 		_err = syscall.Errno(-e.res)
	// 	} else {
	// 		r = int(e.res)
	// 	}
	// 	if r > 0 && dat.op == OP_READ {
	// 		C.memmove(unsafe.Pointer(&dat.buff[0]), dat.cmem, C.size_t(e.res))
	// 	}
	// 	dat.cb(r, _err)
	// 	C.free(dat.cmem)
	// 	data.Delete()

	// 	C.io_uring_cqe_seen(ring, e)
	// }

}
