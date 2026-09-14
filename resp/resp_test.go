package resp

import (
	"bytes"
	"io"
	"testing"
)

func TestReadArrayOfBulkStrings(t *testing.T) {
	raw := "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n"
	r := NewReader(bytes.NewBufferString(raw))

	v, err := r.Read()
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if v.Type != Array || len(v.Elems) != 3 {
		t.Fatalf("Read() = %+v; want Array of 3", v)
	}
	want := []string{"SET", "a", "b"}
	for i, e := range v.Elems {
		if e.Type != BulkString || e.Str != want[i] {
			t.Fatalf("Elems[%d] = %+v; want BulkString %q", i, e, want[i])
		}
	}
}

func TestReadSimpleTypes(t *testing.T) {
	cases := []struct {
		raw  string
		typ  Type
		str  string
		num  int64
	}{
		{"+PONG\r\n", SimpleString, "PONG", 0},
		{"-ERR bad\r\n", Error, "ERR bad", 0},
		{":42\r\n", Integer, "", 42},
	}
	for _, c := range cases {
		r := NewReader(bytes.NewBufferString(c.raw))
		v, err := r.Read()
		if err != nil {
			t.Fatalf("Read(%q) error = %v", c.raw, err)
		}
		if v.Type != c.typ || v.Str != c.str || v.Num != c.num {
			t.Fatalf("Read(%q) = %+v; want Type=%v Str=%q Num=%d", c.raw, v, c.typ, c.str, c.num)
		}
	}
}

func TestReadNullBulkAndArray(t *testing.T) {
	r := NewReader(bytes.NewBufferString("$-1\r\n*-1\r\n"))

	v, err := r.Read()
	if err != nil || v.Type != BulkString || !v.IsNull {
		t.Fatalf("Read() = %+v, err=%v; want null BulkString", v, err)
	}

	v, err = r.Read()
	if err != nil || v.Type != Array || !v.IsNull {
		t.Fatalf("Read() = %+v, err=%v; want null Array", v, err)
	}
}

func TestReadUnknownType(t *testing.T) {
	r := NewReader(bytes.NewBufferString("!bogus\r\n"))
	if _, err := r.Read(); err != ErrUnknownType {
		t.Fatalf("Read() error = %v; want ErrUnknownType", err)
	}
}

func TestReadEOF(t *testing.T) {
	r := NewReader(bytes.NewBufferString(""))
	if _, err := r.Read(); err != io.EOF {
		t.Fatalf("Read() on empty stream error = %v; want io.EOF", err)
	}
}

func TestWriteAndReadRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	if err := w.WriteSimpleString("OK"); err != nil {
		t.Fatalf("WriteSimpleString error = %v", err)
	}
	if err := w.WriteError("ERR bad"); err != nil {
		t.Fatalf("WriteError error = %v", err)
	}
	if err := w.WriteInteger(7); err != nil {
		t.Fatalf("WriteInteger error = %v", err)
	}
	if err := w.WriteBulkString("hello"); err != nil {
		t.Fatalf("WriteBulkString error = %v", err)
	}
	if err := w.WriteNullBulk(); err != nil {
		t.Fatalf("WriteNullBulk error = %v", err)
	}
	if err := w.WriteArray([]Value{
		{Type: BulkString, Str: "a"},
		{Type: Integer, Num: 1},
	}); err != nil {
		t.Fatalf("WriteArray error = %v", err)
	}

	r := NewReader(&buf)

	v, _ := r.Read()
	if v.Type != SimpleString || v.Str != "OK" {
		t.Fatalf("round-trip SimpleString = %+v", v)
	}
	v, _ = r.Read()
	if v.Type != Error || v.Str != "ERR bad" {
		t.Fatalf("round-trip Error = %+v", v)
	}
	v, _ = r.Read()
	if v.Type != Integer || v.Num != 7 {
		t.Fatalf("round-trip Integer = %+v", v)
	}
	v, _ = r.Read()
	if v.Type != BulkString || v.Str != "hello" {
		t.Fatalf("round-trip BulkString = %+v", v)
	}
	v, _ = r.Read()
	if v.Type != BulkString || !v.IsNull {
		t.Fatalf("round-trip NullBulk = %+v", v)
	}
	v, _ = r.Read()
	if v.Type != Array || len(v.Elems) != 2 || v.Elems[0].Str != "a" || v.Elems[1].Num != 1 {
		t.Fatalf("round-trip Array = %+v", v)
	}
}
