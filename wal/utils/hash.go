package utils

import "github.com/spaolacci/murmur3"

func MurmurHash(data []byte) uint32 {
	return murmur3.Sum32(data)
}
