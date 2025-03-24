// Go benchmark for mmap sequential vs random access with read & write
package main

import (
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	FileName   = "mmap_bench.dat"
	ElementNum = 200_000_000
	IntSize    = 8 // uint64
)

func createTestFile() {
	f, err := os.Create(FileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	err = f.Truncate(int64(ElementNum * IntSize))
	if err != nil {
		panic(err)
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, ElementNum*IntSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	defer syscall.Munmap(data)

	slice := *(*[]uint64)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&data[0])),
		Len:  ElementNum,
		Cap:  ElementNum,
	}))

	for i := 0; i < ElementNum; i++ {
		slice[i] = uint64(i + 1)
	}
}

func loadWindow(start, length int, mmap []byte) []uint64 {
	slice := *(*[]uint64)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&mmap[start*IntSize])),
		Len:  length,
		Cap:  length,
	}))
	return slice
}

func benchmarkSequential(mmap []byte) {
	fmt.Println("-- [CASE 1] Sequential access (100M full preload)")
	start := time.Now()

	slice := loadWindow(0, 100_000_000, mmap)

	// Advise OS to preload memory
	err := unix.Madvise(mmap[0:100_000_000*IntSize], unix.MADV_WILLNEED)
	if err != nil {
		fmt.Println("madvise error:", err)
	}

	// Force actual memory loading (page fault)
	for i := 0; i < len(slice); i += 4096 / IntSize {
		_ = slice[i]
	}

	fmt.Println("Preloaded 100M elements in:", time.Since(start))

	start = time.Now()
	for i := 0; i < 100_000_000; i++ {
		idx := rand.Intn(len(slice))
		slice[idx] = 999999999
	}
	fmt.Println("Write 100,000 within slice done in:", time.Since(start))

	for i := 0; i < 10; i++ {
		idx := rand.Intn(len(slice))
		fmt.Printf("AfterWrite[%d] = %d\n", idx, slice[idx])
	}
}

func benchmarkRandom(mmap []byte) {
	fmt.Println("-- [CASE 2] Random access (1M lazy demand-paging)")

	total := 100_000_000
	indices := make([]int, total)
	for i := range indices {
		indices[i] = rand.Intn(ElementNum)
	}

	// Just read a few samples before writing
	for i := 0; i < 10; i++ {
		idx := indices[rand.Intn(total)]
		val := *(*uint64)(unsafe.Pointer(&mmap[idx*IntSize]))
		fmt.Printf("InitialRead[%d] = %d\n", idx, val)
	}

	start := time.Now()
	for i := 0; i < total; i++ {
		idx := indices[i]
		ptr := (*uint64)(unsafe.Pointer(&mmap[idx*IntSize]))
		*ptr = 888888888888
	}
	fmt.Println("Write 1M random individual done in:", time.Since(start))

	for i := 0; i < 10; i++ {
		idx := indices[rand.Intn(total)]
		val := *(*uint64)(unsafe.Pointer(&mmap[idx*IntSize]))
		fmt.Printf("AfterWrite[%d] = %d\n", idx, val)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	if _, err := os.Stat(FileName); os.IsNotExist(err) {
		fmt.Println("Generating test file...")
		createTestFile()
	}

	file, err := os.OpenFile(FileName, os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	mmap, err := syscall.Mmap(int(file.Fd()), 0, ElementNum*IntSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	defer syscall.Munmap(mmap)

	benchmarkSequential(mmap)
	benchmarkRandom(mmap)
}
