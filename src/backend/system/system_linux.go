package system

import "golang.org/x/sys/unix"

func AllocSlab(size int) ([]byte, error) {
	raw, err := unix.Mmap(-1,
		0, int(size), 
		unix.PROT_READ | unix.PROT_WRITE,
		unix.MAP_ANON  | unix.MAP_PRIVATE,
	) 

	return raw, err
}

func DeallocSlab(ptr []byte) error {
	err := unix.Munmap(ptr)
	return err
}

