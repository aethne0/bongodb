package util

import (
	c "bongodb/src"
	"encoding/binary"
	"fmt"
)

func PrintBytes(raw *[c.PAGE_SIZE]byte) {
	PrintBytesCfg(raw, 2, 16)
}

func PrintBytesCfg(raw *[c.PAGE_SIZE]byte, group int, cols int) {
	fmt.Print("        ")
	for i := range group * cols {
		fmt.Printf("%02x", i)
		// 1. Handle grouping (space between byte groups)
		if group > 0 && (i+1)%group == 0 {
			fmt.Print(" ")
		}
	}
	fmt.Println()
	fmt.Println()

	for i := range c.PAGE_SIZE {
		if i == 0x40 { // TODO:
			fmt.Println()
		}
		if i%(group*cols) == 0 {
			fmt.Printf("+%04x | ", i)
		}

		// Print the byte in hex
		fmt.Printf("%02x", raw[i])

		// 1. Handle grouping (space between byte groups)
		if group > 0 && (i+1)%group == 0 {
			fmt.Print(" ")
		}

		// 2. Handle columns (newline after N groups)
		// We multiply group * cols to find the total bytes per line
		if cols > 0 && (i+1)%(group*cols) == 0 {
			fmt.Println()
		}
	}
	fmt.Println() // Final newline
}

func PrettyPrintPage(data []byte, limit int) {
	if limit > len(data) {
		limit = len(data)
	}

	const bytesPerRow = 32
	fmt.Println("┏━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓")
	fmt.Printf("┃ Offset ┃ u16 Chunks (BigEndian) - %5d bytes (0x%04x)                                       ┃\n",
		c.PAGE_SIZE, c.PAGE_SIZE)
	fmt.Println("┣━━━━━━━━╋━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫")

	for i := 0; i < limit; i += bytesPerRow {
		if i < 0x40 {
			fmt.Printf("┃ 0x%04x ┣ ", i)
		} else {
			fmt.Printf("┃ 0x%04x ┃ ", i)
		}

		for j := 0; j < bytesPerRow; j += 2 {
			if i+j+1 < limit {
				val := binary.BigEndian.Uint16(data[i+j : i+j+2])
				fmt.Printf("%04x ", val)

			}
			// Space every 8 bytes to keep your eyes from crossing
			if (j+2)%8 == 0 {
				fmt.Print(" ")
			}
		}
		if i < 0x40 {
			fmt.Println("┫")
		} else {
			fmt.Println("┃")
		}
	}
	fmt.Println("┗━━━━━━━━┻━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛")
}
