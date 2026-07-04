package utils

import "io"

func WriteFull(w io.Writer, data []byte) (int, error) {
	var bytesWritten int

	for len(data) > 0 {
		n, err := w.Write(data)
		bytesWritten += n
		if err != nil {
			return bytesWritten, err
		}

		data = data[n:]
	}

	return bytesWritten, nil
}
