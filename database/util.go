package database

import (
	"fmt"
	"hash/crc32"
)

const advisoryLockIdSalt uint = 1486364155

// inspired by rails migrations, see https://goo.gl/8o9bCT
func GenerateAdvisoryLockId(databaseName string) (string, error) {
	sum := crc32.ChecksumIEEE([]byte(databaseName))
	sum = sum * uint32(advisoryLockIdSalt)
	return fmt.Sprintf("%v", sum), nil
}

// SplitQuery splits migration contents by ';' with considering quotes.
func SplitQuery(buf []byte) [][]byte {
	queries := make([][]byte, 0, 8)
	last := 0
	var escaped, quoted bool
	var quote byte
	for i := 0; i <= len(buf); i++ {
		if i == last && (i == len(buf) || buf[i] == ' ' || buf[i] == '\n') {
			last = i + 1
			continue
		}
		if i == len(buf) || buf[i] == ';' && !escaped && !quoted {
			queries = append(queries, buf[last:i])
			last = i + 1
			continue
		}
		if escaped {
			escaped = false
		} else {
			escaped = buf[i] == '\\'
			if quoted {
				if quote == buf[i] {
					quoted = false
				}
			} else if buf[i] == '"' || buf[i] == '\'' {
				quoted = true
				quote = buf[i]
			}
		}
	}
	return queries
}
