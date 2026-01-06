// Constants
package c

import (
	"encoding/binary"
)

const LEN_U16 	= 0x02
const LEN_U32 	= 0x04
const LEN_U64 	= 0x08
const LEN_U128 	= 0x10

const PAGE_SIZE 				= 0x1000 	// must be u16 & power of 2
const PAGE_CACHE_SHARDS 		= uint64(4)
const PAGE_CACHE_SHARD_PAGES 	= uint64(4)
const PAGE_CACHE_SIZE_PAGES 	= PAGE_CACHE_SHARDS * PAGE_CACHE_SHARD_PAGES
const PAGE_CACHE_SIZE_BYTES 	= PAGE_CACHE_SIZE_PAGES * PAGE_SIZE

// This is an alias for endianness effectively, so we only define endianness in one place (here).
// For debugging big endian is easier to visualize, but for "prod" LittleEndian is faster (usually) (probably)
var Bin = binary.BigEndian
