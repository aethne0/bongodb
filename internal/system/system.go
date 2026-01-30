// Platform abstracted filesystem ops
package system

// DiskOp is not owned my Iomgr
type DiskOp struct {
	opcode	OpCode

	bufptr	uintptr // pointer to start of buf - len is implictly PAGE_SIZE
	offset	uint64 	// target file offset

	Res		int32
	Ch		chan struct{} // set by caller

	_ [24]byte // pad to 128 bytes
}

// we can make this smaller if we need space, but we are padding now anyway
type OpCode uint32
const (
	OpNop 	OpCode = iota
	OpWrite 
	OpRead
	OpSync
	OpAllocate
	// OpTruncate
)