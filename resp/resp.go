// Package resp implements a minimal reader/writer for the Redis
// Serialization Protocol (RESP), enough to speak to a real redis-cli.
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// Type is the leading byte of a RESP frame, which tells us how to
// decode/encode the rest of the line(s) that follow it.
type Type byte

const (
	SimpleString Type = '+'
	Error        Type = '-'
	Integer      Type = ':'
	BulkString   Type = '$'
	Array        Type = '*'
)

// Value is a parsed RESP message. Only the fields relevant to Type are
// populated; e.g. an Array populates Elems, a BulkString populates Str.
type Value struct {
	Type   Type
	Str    string
	Num    int64
	Elems  []Value
	IsNull bool
}

var ErrUnknownType = errors.New("resp: unknown type byte")

// Reader decodes a stream of RESP values from an underlying connection.
type Reader struct {
	br *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReader(r)}
}

// readLine reads bytes up to (and stripping) the trailing \r\n.
func (r *Reader) readLine() (string, error) {
	line, err := r.br.ReadString('\n')
	if err != nil {
		return "", err
	}
	// Strip trailing \r\n or \n.
	n := len(line)
	if n >= 2 && line[n-2] == '\r' {
		return line[:n-2], nil
	}
	return line[:n-1], nil
}

// Read decodes the next RESP value from the stream. It is called
// recursively to decode nested arrays.
func (r *Reader) Read() (Value, error) {
	typeByte, err := r.br.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch Type(typeByte) {
	case SimpleString:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		return Value{Type: SimpleString, Str: line}, nil

	case Error:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		return Value{Type: Error, Str: line}, nil

	case Integer:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return Value{}, fmt.Errorf("resp: invalid integer %q: %w", line, err)
		}
		return Value{Type: Integer, Num: n}, nil

	case BulkString:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		length, err := strconv.Atoi(line)
		if err != nil {
			return Value{}, fmt.Errorf("resp: invalid bulk length %q: %w", line, err)
		}
		if length == -1 {
			return Value{Type: BulkString, IsNull: true}, nil
		}
		buf := make([]byte, length+2) // +2 for trailing \r\n
		if _, err := io.ReadFull(r.br, buf); err != nil {
			return Value{}, err
		}
		return Value{Type: BulkString, Str: string(buf[:length])}, nil

	case Array:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		count, err := strconv.Atoi(line)
		if err != nil {
			return Value{}, fmt.Errorf("resp: invalid array length %q: %w", line, err)
		}
		if count == -1 {
			return Value{Type: Array, IsNull: true}, nil
		}
		elems := make([]Value, count)
		for i := 0; i < count; i++ {
			v, err := r.Read()
			if err != nil {
				return Value{}, err
			}
			elems[i] = v
		}
		return Value{Type: Array, Elems: elems}, nil

	default:
		return Value{}, ErrUnknownType
	}
}

// Writer encodes RESP values back to the client.
type Writer struct {
	bw *bufio.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriter(w)}
}

func (w *Writer) WriteSimpleString(s string) error {
	_, err := fmt.Fprintf(w.bw, "+%s\r\n", s)
	if err != nil {
		return err
	}
	return w.bw.Flush()
}

func (w *Writer) WriteError(msg string) error {
	_, err := fmt.Fprintf(w.bw, "-%s\r\n", msg)
	if err != nil {
		return err
	}
	return w.bw.Flush()
}

func (w *Writer) WriteInteger(n int64) error {
	_, err := fmt.Fprintf(w.bw, ":%d\r\n", n)
	if err != nil {
		return err
	}
	return w.bw.Flush()
}

func (w *Writer) WriteBulkString(s string) error {
	_, err := fmt.Fprintf(w.bw, "$%d\r\n%s\r\n", len(s), s)
	if err != nil {
		return err
	}
	return w.bw.Flush()
}

// WriteNullBulk writes the RESP nil reply ($-1\r\n), used e.g. for GET
// on a missing key.
func (w *Writer) WriteNullBulk() error {
	_, err := w.bw.WriteString("$-1\r\n")
	if err != nil {
		return err
	}
	return w.bw.Flush()
}

func (w *Writer) WriteArray(values []Value) error {
	if _, err := fmt.Fprintf(w.bw, "*%d\r\n", len(values)); err != nil {
		return err
	}
	for _, v := range values {
		if err := w.writeValueNoFlush(v); err != nil {
			return err
		}
	}
	return w.bw.Flush()
}

// writeValueNoFlush writes an arbitrary already-constructed Value,
// used internally for encoding nested array elements.
func (w *Writer) writeValueNoFlush(v Value) error {
	switch v.Type {
	case SimpleString:
		_, err := fmt.Fprintf(w.bw, "+%s\r\n", v.Str)
		return err
	case Error:
		_, err := fmt.Fprintf(w.bw, "-%s\r\n", v.Str)
		return err
	case Integer:
		_, err := fmt.Fprintf(w.bw, ":%d\r\n", v.Num)
		return err
	case BulkString:
		if v.IsNull {
			_, err := w.bw.WriteString("$-1\r\n")
			return err
		}
		_, err := fmt.Fprintf(w.bw, "$%d\r\n%s\r\n", len(v.Str), v.Str)
		return err
	case Array:
		if v.IsNull {
			_, err := w.bw.WriteString("*-1\r\n")
			return err
		}
		if _, err := fmt.Fprintf(w.bw, "*%d\r\n", len(v.Elems)); err != nil {
			return err
		}
		for _, e := range v.Elems {
			if err := w.writeValueNoFlush(e); err != nil {
				return err
			}
		}
		return nil
	default:
		return ErrUnknownType
	}
}
