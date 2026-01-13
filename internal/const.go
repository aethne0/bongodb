// Constants
package c

import (
	"encoding/binary"
)

const LEN_U16 	= 0x02
const LEN_U32 	= 0x04
const LEN_U64 	= 0x08
const LEN_U128 	= 0x10

const _OS_PAGE					= 0x1000
const PAGE_SIZE 				= _OS_PAGE * 1

// This is an alias for endianness effectively, so we only define endianness in one place (here).
// For debugging big endian is easier to visualize, but for "prod" LittleEndian is faster (usually) (probably)
var Bin = binary.BigEndian
