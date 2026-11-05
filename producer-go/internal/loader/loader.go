package loader

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

type Record struct {
	ID            string `json:"id"`
	Text          string `json:"text"`
	SourceDataset string `json:"source_dataset"`
}

// LoadJSONL reads a JSONL file where each line is a Record.
func LoadJSONL(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening data file: %w", err)
	}
	defer f.Close()

	var records []Record
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("parsing line: %w", err)
		}
		records = append(records, rec)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning data file: %w", err)
	}
	return records, nil
}
