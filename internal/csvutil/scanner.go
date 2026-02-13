package csvutil

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ScanDirectory returns all .csv file paths found in dir (non-recursive).
func ScanDirectory(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading directory %s: %w", dir, err)
	}

	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.EqualFold(filepath.Ext(e.Name()), ".csv") {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	return files, nil
}
