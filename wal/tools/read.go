package tools

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

func ReadIndex(index string) error {
	iFile, err := os.OpenFile(index, os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	indexFileStat, err := os.Stat(index)
	if err != nil {
		return err
	}

	totalMsgs := indexFileStat.Size() / 12

	for offset := 0; offset < int(totalMsgs); offset++ {
		posInIdx := int64(offset * 12)
		posInIdx += 4

		data := make([]byte, 8)
		n, err := iFile.ReadAt(data, posInIdx)
		if err != nil {
			return err
		}

		if n < 8 {
			return errors.New("unable to read position bytes")
		}

		posInStore := binary.BigEndian.Uint64(data)

		fmt.Printf("Offset: %d \t PosInStore: %d\n", offset, posInStore)
	}

	return nil
}
