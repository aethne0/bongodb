//go:build linux
package iomgr

import (
	"fmt"
	"strings"
	"unsafe"
)

func (o *Op) String() string {
	if o == nil {
		return "<nil>"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Op | Opcode: %v, Fd: 0x%x, Done: %v, Count: %d, Seen: %d, Res: 0x%x | Ch: @0x%x\n", 
		o.Opcode, o.Fd, o.done, o.Count, o.seen, o.Res, unsafe.Pointer(&o.Ch))
	
	switch o.Opcode {
	case OpWrite:
		for i := range min(OP_MAX_OPS, o.Count) {
			var d string
			if i + 1 == o.seen {
				d = ">"
			} else {
				d = "|"
			}
			fmt.Fprintf(&b, "   %s [%02d] WRITE     [ Buf: @0x%x | Len: 0x%08x | Off: 0x%08x]\n", 
				d, i, o.Bufs[i], o.Lens[i], o.Offs[i])
		}
		if o.Sync {
			var d string
			if o.seen == o.Count {
				d = ">"
			} else {
				d = "|"
			}
			fmt.Fprintf(&b, "   %s [%02d] FSYNC     [ ]\n", d, min(OP_MAX_OPS, o.Count))
		}
	case OpRead:
		for i := range min(OP_MAX_OPS, o.Count) {
			var d string
			if i + 1 == o.seen {
				d = ">"
			} else {
				d = "|"
			}
			fmt.Fprintf(&b, "   %s [%02d] READ      [ Buf: @0x%x | Len: 0x%08x | Off: 0x%08x]\n", 
				d, i, o.Bufs[i], o.Lens[i], o.Offs[i])
		}

	case OpSync:
			fmt.Fprintf(&b, "   > [%02d] FYSNC     [ ]\n", 0)
	case OpAllocate:
			fmt.Fprintf(&b, "   > [%02d] FALLOCATE [ ]\n", 0)
	}

	return b.String()
}

