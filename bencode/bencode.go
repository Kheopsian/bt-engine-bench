// Package bencode implements just enough of the BitTorrent bencode format
// to write .torrent files. We do not need a decoder for the bench harness
// (engines parse the file, we only emit it), so the API is intentionally
// minimal: pass it a Value tree, get bytes back.
//
// Reference: https://wiki.theory.org/BitTorrentSpecification#Bencoding
package bencode

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
)

// Value is a single bencode value. Concrete types accepted by Encode are:
//   - string         → bytestring
//   - []byte         → bytestring (use this for raw piece hashes)
//   - int / int64    → integer
//   - []Value        → list
//   - Dict           → dictionary (string keys; emitted in sorted order)
//
// Anything else triggers a non-nil error.
type Value any

// Dict is a bencode dictionary. Keys are encoded as bytestrings; the spec
// requires lexicographic key ordering so callers do not have to pre-sort.
type Dict map[string]Value

// Encode bencodes v and returns the resulting bytes.
func Encode(v Value) ([]byte, error) {
	var buf bytes.Buffer
	if err := writeValue(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeValue(buf *bytes.Buffer, v Value) error {
	switch x := v.(type) {
	case string:
		writeBytes(buf, []byte(x))
	case []byte:
		writeBytes(buf, x)
	case int:
		writeInt(buf, int64(x))
	case int64:
		writeInt(buf, x)
	case []Value:
		buf.WriteByte('l')
		for _, e := range x {
			if err := writeValue(buf, e); err != nil {
				return err
			}
		}
		buf.WriteByte('e')
	case Dict:
		return writeDict(buf, x)
	default:
		return fmt.Errorf("bencode: unsupported type %T", v)
	}
	return nil
}

func writeBytes(buf *bytes.Buffer, b []byte) {
	buf.WriteString(strconv.Itoa(len(b)))
	buf.WriteByte(':')
	buf.Write(b)
}

func writeInt(buf *bytes.Buffer, n int64) {
	buf.WriteByte('i')
	buf.WriteString(strconv.FormatInt(n, 10))
	buf.WriteByte('e')
}

func writeDict(buf *bytes.Buffer, d Dict) error {
	keys := make([]string, 0, len(d))
	for k := range d {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	buf.WriteByte('d')
	for _, k := range keys {
		writeBytes(buf, []byte(k))
		if err := writeValue(buf, d[k]); err != nil {
			return err
		}
	}
	buf.WriteByte('e')
	return nil
}
