package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

const (
	RDB_MAGIC_STRING = "REDIS"
	RDB_VERSION      = "0012"
	SELECT_DB        = 0xFE
	DB_SELECTOR      = 0x00
	RDB_EOF          = 0xFF
	AUXILIARY_FIELD  = 0xFA
)

func LoadRDBFile(dir, dbFilename string, store Store) error {
	if dbFilename == "" {
		fmt.Println("dbFileName is empty Skipping RDB load.")
		return nil // no file to load
	}

	path := filepath.Join(dir, dbFilename)
	absPath, err := filepath.Abs(path)
	if err != nil {
		fmt.Printf("failed to resolve RDB path: %s", err)
		return nil
	}
	file, err := os.Open(absPath)
	if err != nil {
		fmt.Printf("Failed to open RDB file at %s: %s\n", absPath, err)
		return nil
	}
	defer file.Close()

	return parseRDB(file, store)
}

func parseRDB(reader io.Reader, store Store) error {
	visitor := &RDBStoreVisitor{store: store}
	_, err := parseHeader(visitor).
		Next(parseMetadata(visitor)).
		Next(parseDb(visitor))(reader)
	return err
}

func readLengthPrefixedString(r io.Reader) (string, error) {
	var lenByte [1]byte
	if _, err := io.ReadFull(r, lenByte[:]); err != nil {
		return "", err
	}

	strLen := int(lenByte[0]) // Simplified: assume < 64 bytes (6-bit encoding)
	buf := make([]byte, strLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}

	return string(buf), nil
}

type ParserFunc func(io.Reader) (ParserFunc, error)

func (f ParserFunc) Next(next ParserFunc) ParserFunc {
	return func(r io.Reader) (ParserFunc, error) {
		_, err := f(r)
		if err != nil {
			return nil, err
		}
		return next(r)
	}
}

func parseHeader(visitor RDBVisitor) ParserFunc {
	return func(r io.Reader) (ParserFunc, error) {
		header := make([]byte, 9)
		if _, err := io.ReadFull(r, header); err != nil {
			return nil, fmt.Errorf("read header: %w", err)
		}

		if string(header[:5]) != RDB_MAGIC_STRING {
			return nil, fmt.Errorf("invalid header prefix: %s", string(header[:5]))
		}

		versionStr := string(header[5:])
		version, err := strconv.Atoi(versionStr)
		if err != nil {
			return nil, fmt.Errorf("invalid version: %s", versionStr)
		}

		visitor.OnHeader(version)
		return nil, nil
	}
}

func parseMetadata(visitor RDBVisitor) ParserFunc {
	return func(reader io.Reader) (ParserFunc, error) {
		for {
			byteVal, reader, err := peekNBytes(reader, 1)
			if err != nil {
				return nil, err
			}

			if byteVal[0] != AUXILIARY_FIELD {
				return nil, nil
			}

			// after peeking we need to consume the byte value
			_, err = reader.Read(make([]byte, 1))
			if err != nil {
				return nil, err
			}

			key, err := readLengthPrefixedString(reader)
			if err != nil {
				return nil, err
			}
			val, err := readLengthPrefixedString(reader)
			if err != nil {
				return nil, err
			}

			visitor.OnAuxField(key, val)
		}
	}
}

func parseDb(visitor RDBVisitor) ParserFunc {
	return func(reader io.Reader) (ParserFunc, error) {
		for {
			byteIndicator := make([]byte, 1)
			if _, err := reader.Read(byteIndicator); err != nil {
				return nil, err
			}

			switch byteIndicator[0] {
			case SELECT_DB: // SELECTDB
				dbIndexRaw := make([]byte, 1) // Simplified
				reader.Read(dbIndexRaw)
				visitor.OnDBStart(int(dbIndexRaw[0]))

			case DB_SELECTOR: // string key
				key, _ := readLengthPrefixedString(reader)
				val, _ := readLengthPrefixedString(reader)
				visitor.OnEntry(key, val, int64(InfiniteTTL))

			case RDB_EOF: // EOF
				return nil, nil

			default:
				return nil, fmt.Errorf("unsupported type byte: 0x%02X", byteIndicator[0])
			}
		}
	}
}

func peekNBytes(reader io.Reader, n int) ([]byte, io.Reader, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(reader, buf)
	if err != nil {
		return nil, nil, err
	}

	// Prepend the peeked bytes back to the reader
	newReader := io.MultiReader(bytes.NewReader(buf), reader)
	return buf, newReader, nil
}
