package csvutil

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

// Reader wraps a csv.Reader and provides chunked batch reading.
type Reader struct {
	file    *os.File
	reader  *csv.Reader
	headers []string
}

// NewReader opens the CSV file and reads the header row.
func NewReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening csv: %w", err)
	}

	r := csv.NewReader(f)
	r.LazyQuotes = true
	r.ReuseRecord = false

	headers, err := r.Read()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("reading csv headers: %w", err)
	}

	return &Reader{
		file:    f,
		reader:  r,
		headers: headers,
	}, nil
}

// Headers returns the CSV column headers.
func (r *Reader) Headers() []string {
	return r.headers
}

// ReadBatch reads up to n rows from the CSV. Returns the rows read and io.EOF
// when the file is exhausted. A final partial batch is returned with io.EOF.
func (r *Reader) ReadBatch(n int) ([][]string, error) {
	batch := make([][]string, 0, n)
	for range n {
		record, err := r.reader.Read()
		if err == io.EOF {
			return batch, io.EOF
		}
		if err != nil {
			return batch, fmt.Errorf("reading csv row: %w", err)
		}
		batch = append(batch, record)
	}
	return batch, nil
}

// Close closes the underlying file.
func (r *Reader) Close() error {
	return r.file.Close()
}
