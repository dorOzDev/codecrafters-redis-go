package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"

	"github.com/taroim/rdb/lzf"
)

const (
	RDB_MAGIC_STRING = "REDIS"
	RDB_VERSION      = "0012"
	SELECT_DB        = 0xFE
	DB_SELECTOR      = 0x00
	RDB_EOF          = 0xFF
	AUXILIARY_FIELD  = 0xFA

	TWO_MOST_SIGINFICANT_BITS = 0xC0
)

func LoadRDBFile(dir, dbFilename string, store Store) error {
	if dbFilename == "" {
		log.Println("dbFileName is empty Skipping RDB load.")
		return nil // no file to load
	}

	path := filepath.Join(dir, dbFilename)
	absPath, err := filepath.Abs(path)
	if err != nil {
		log.Printf("failed to resolve RDB path: %s", err)
		return nil
	}
	file, err := os.Open(absPath)
	if err != nil {
		log.Printf("Failed to open RDB file at %s: %s\n", absPath, err)
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

func readRdbString(reader io.Reader) (string, error) {
	enc, err := readLengthEncoded(reader)
	if err != nil {
		return "", err
	}

	if enc.Mode == LengthEncodingSpecial {
		return readSpecialFormat(reader, *enc.StringEncType)
	}

	return readStringOfLength(reader, enc.Value)
}

type LengthEncodingModeEnum int

const (
	LengthEncoding6Bit LengthEncodingModeEnum = iota
	LengthEncoding14Bit
	LengthEncoding32Bit
	LengthEncodingSpecial
)

type StringEncodingEnum int

const (
	StringEncodingInt8 StringEncodingEnum = iota
	StringEncodingInt16
	StringEncodingInt32
	StringEncodingLZF
)

type LengthEncoding struct {
	Mode          LengthEncodingModeEnum
	BitLength     int
	Value         int
	StringEncType *StringEncodingEnum
}

func (e StringEncodingEnum) String() string {
	switch e {
	case StringEncodingInt8:
		return "int8"
	case StringEncodingInt16:
		return "int16"
	case StringEncodingInt32:
		return "int32"
	case StringEncodingLZF:
		return "lzf"
	default:
		return "unknown"
	}
}

func readLengthEncoded(reader io.Reader) (*LengthEncoding, error) {
	firstByte, err := readNBytes(reader, 1)
	if err != nil {
		return nil, err
	}
	b := firstByte[0]
	mode := (b & 0xC0) >> 6

	switch mode {
	case 0: // 6-bit
		length := int(b & 0x3F)
		return &LengthEncoding{
			Mode:      LengthEncoding6Bit,
			BitLength: 6,
			Value:     length,
		}, nil

	case 1: // 14-bit
		nextByte, err := readNBytes(reader, 1)
		if err != nil {
			return nil, err
		}
		length := ((int(b) & 0x3F) << 8) | int(nextByte[0])
		return &LengthEncoding{
			Mode:      LengthEncoding14Bit,
			BitLength: 14,
			Value:     length,
		}, nil

	case 2: // 32-bit
		lenBytes, err := readNBytes(reader, 4)
		if err != nil {
			return nil, err
		}
		length := int(binary.BigEndian.Uint32(lenBytes))
		return &LengthEncoding{
			Mode:      LengthEncoding32Bit,
			BitLength: 32,
			Value:     length,
		}, nil

	case 3:
		val, err := getStringEncodingEnum(firstByte[0])
		if err != nil {
			return nil, err

		}

		return &LengthEncoding{
			Mode:          LengthEncodingSpecial,
			StringEncType: &val,
		}, nil

	default:
		return nil, fmt.Errorf("invalid length encoding mode: %d", mode)
	}
}

func getStringEncodingEnum(encTypeByte byte) (StringEncodingEnum, error) {
	encType := encTypeByte & 0x3F

	switch encType {
	case 0x00:
		return StringEncodingInt8, nil
	case 0x01:
		return StringEncodingInt16, nil
	case 0x02:
		return StringEncodingInt32, nil
	case 0x03:
		return StringEncodingLZF, nil
	default:
		return 0, fmt.Errorf("unknown special string encoding type: %d", encTypeByte)
	}
}

func readSpecialFormat(reader io.Reader, encodeType StringEncodingEnum) (string, error) {
	switch encodeType {
	case StringEncodingInt8:
		num, err := readNBytes(reader, 1)
		if err != nil {
			return "", err
		}
		return strconv.Itoa(int(int8(num[0]))), nil
	case StringEncodingInt16:
		num, err := readNBytes(reader, 1)
		if err != nil {
			return "", err
		}
		val := int16(binary.BigEndian.Uint16(num))
		return strconv.Itoa(int(val)), nil
	case StringEncodingInt32:
		num, err := readNBytes(reader, 4)
		if err != nil {
			return "", err
		}
		val := int32(binary.BigEndian.Uint32(num))
		return strconv.Itoa(int(val)), nil
	case StringEncodingLZF:
		return readCompressedString(reader)
	}

	return "", fmt.Errorf("unsupported encoding type: %d", encodeType)
}

func readCompressedString(reader io.Reader) (string, error) {
	compressedLen, err := readLengthEncoded(reader)
	if err != nil {
		return "", err
	}

	originalLen, err := readLengthEncoded(reader)
	if err != nil {
		return "", err
	}

	compressedData, err := readNBytes(reader, compressedLen.Value)
	if err != nil {
		return "", err
	}

	if compressedLen.Value != len(compressedData) {
		return "", fmt.Errorf("compressed data length mismatch: expected %d, got %d", compressedLen.Value, len(compressedData))
	}

	decompressed, err := lzf.Decompress(compressedData, compressedLen.Value, originalLen.Value)
	if err != nil {
		return "", fmt.Errorf("LZF decompression failed: %w", err)
	}

	if originalLen.Value != len(decompressed) {
		return "", fmt.Errorf("original data length mismatch: expected %d, got %d", originalLen.Value, len(decompressed))
	}

	return string(decompressed), nil
}

func readNBytes(reader io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return nil, err
	}

	return buf, nil
}

func readStringOfLength(reader io.Reader, length int) (string, error) {
	buf := make([]byte, length)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

/**
 * trigger the rdb secion parseer and return the updated reader with updated cursor position
 * @return the updated Reader(with most recent cursor position)
 */
type ParserFunc func(io.Reader) (io.Reader, error)

func (f ParserFunc) Next(next ParserFunc) ParserFunc {
	return func(r io.Reader) (io.Reader, error) {
		updatedReader, err := f(r)
		if err != nil {
			return nil, err
		}
		return next(updatedReader)
	}
}

func parseHeader(visitor RDBVisitor) ParserFunc {
	return func(reader io.Reader) (io.Reader, error) {
		header := make([]byte, 9)
		if _, err := io.ReadFull(reader, header); err != nil {
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
		return reader, nil
	}
}

func parseMetadata(visitor RDBVisitor) ParserFunc {
	return func(reader io.Reader) (io.Reader, error) {
		for {
			byteVal, reader, err := peekNBytes(reader, 1)
			if err != nil {
				return nil, err
			}

			if byteVal[0] != AUXILIARY_FIELD {
				return reader, nil
			}

			// after peeking we need to consume the byte value
			_, err = reader.Read(make([]byte, 1))
			if err != nil {
				return nil, err
			}

			key, err := readRdbString(reader)
			if err != nil {
				return nil, err
			}
			val, err := readRdbString(reader)
			if err != nil {
				return nil, err
			}

			visitor.OnAuxField(key, val)
		}
	}
}

func parseDb(visitor RDBVisitor) ParserFunc {
	return func(reader io.Reader) (io.Reader, error) {
		var expireAt *int64 = nil // nil means no expiration

		for {
			opcode := make([]byte, 1)
			_, err := reader.Read(opcode) // consume opcode
			if err != nil {
				return nil, err
			}

			switch opcode[0] {
			case 0xFE: // SELECTDB
				dbNumberEnc, err := readLengthEncoded(reader)
				if err != nil {
					return nil, err
				}
				visitor.OnDBStart(dbNumberEnc.Value)

			case 0xFD: // EXPIRETIME_MS
				buf, err := readNBytes(reader, 4)
				if err != nil {
					return nil, err
				}
				raw := binary.LittleEndian.Uint32(buf)
				val := int64(raw)
				expireAt = &val

			case 0xFC: // EXPIRETIME (seconds)
				buf, err := readNBytes(reader, 8)
				if err != nil {
					return nil, err
				}
				raw := binary.LittleEndian.Uint64(buf)
				if raw > math.MaxInt64 {
					return nil, fmt.Errorf("expire time value too large to convert to int64: %d", raw)
				}
				val := int64(raw)
				expireAt = &val

			case 0xFB: // RESIZEDB
				dbSize, err := readLengthEncoded(reader)
				if err != nil {
					return nil, err
				}
				expireSize, err := readLengthEncoded(reader)
				if err != nil {
					return nil, err
				}
				visitor.OnResizeDB(dbSize.Value, expireSize.Value)

			case 0x00: // String key and value
				key, err := readRdbString(reader)
				if err != nil {
					return nil, err
				}
				value, err := readRdbString(reader)
				if err != nil {
					return nil, err
				}
				visitor.OnEntry(key, value, expireAt)

				expireAt = nil

			case 0xFF: // EOF
				// TODO add support for checksum. for now just validate reading to buffer succufully
				_, err := readNBytes(reader, 8)
				if err != nil {
					return nil, fmt.Errorf("unable to read CRC64 checksum: %v", err)
				}
				return nil, nil

			default:
				return nil, fmt.Errorf("unsupported opcode: 0x%02X", opcode[0])
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
