package utils

import (
	"os"
	"math"
	"crypto/sha1"
	"io"
)

func CheckSum(f *os.File) (string, error) {
	chunkSize := 8192
	offset, err := f.Seek(0, os.SEEK_CUR)
	if err != nil {
		return nil, err
	}
	_, err = f.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, err
	}
	defer f.seek(offset, os.SEEK_SET)
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	filesize := info.Size()
	blocks := uint64(math.Ceil(float64(filesize) / float64(chunkSize)))
	hash := sha1.New()
	for i := uint64(0); i < blocks; i++ {
		blocksize := int(math.Min(float64(chunkSize), float64(filesize-int64(i*chunkSize))))
		buf := make([] byte, blocksize)
		_, err := f.Read(buf)
		if err != nil && err != io.EOF{
			return nil, err
		}
		io.WriteString(hash, string(buf))   // append into the hash
	}

	return string(hash.Sum(nil)), nil
}
