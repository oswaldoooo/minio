package sio_test

import (
	"os"
	"testing"

	"github.com/minio/minio/internal/sio"
)

func TestSio(t *testing.T) {
	f, err := sio.OpenFile("test.dat", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	n, err := f.Write([]byte("hello data"))
	if err != nil {
		t.Fatal(err)
	} else if n < 0 {
		t.Fatal("write failed")
	}
	f2, err := sio.OpenFile("test.dat", os.O_RDONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()
	var buff [1 << 10]byte
	n, err = f2.Read(buff[:])
	if err != nil {
		t.Fatal(err)
	} else if n < 0 {
		t.Fatal("read failed")
	}
	if string(buff[:n]) != "hello data" {
		t.Fatal("bad result " + string(buff[:n]))
	}
}
