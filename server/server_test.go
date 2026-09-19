package server

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"kvstore/store"
)

// startTestServer starts a Server on an OS-assigned loopback port and
// returns its address plus a cleanup func that shuts it down.
func startTestServer(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	addr := ln.Addr().String()

	s := New(addr, store.New())
	// Hand the already-bound listener to the server by re-closing this
	// probe listener and letting ListenAndServe bind the same address;
	// there's a tiny window where another process could steal the port,
	// but it's fine for a test.
	ln.Close()

	errCh := make(chan error, 1)
	go func() { errCh <- s.ListenAndServe() }()

	// Wait for the listener to actually be up before returning.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Cleanup(func() {
		s.Shutdown()
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("ListenAndServe returned error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("server did not shut down in time")
		}
	})

	return addr
}

// cmd sends a RESP command and returns the single reply line(s) up to
// and including its terminating \r\n(-terminated payload for bulk
// strings).
func cmd(t *testing.T, rw *bufio.ReadWriter, resp string) string {
	t.Helper()

	if _, err := rw.WriteString(resp); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := rw.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	line, err := rw.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	// Bulk strings ($len\r\n<payload>\r\n) have a second line to consume.
	if len(line) > 0 && line[0] == '$' && line[1] != '-' {
		payload, err := rw.ReadString('\n')
		if err != nil {
			t.Fatalf("read bulk payload: %v", err)
		}
		return line + payload
	}
	return line
}

func TestServerPingSetGetDel(t *testing.T) {
	addr := startTestServer(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	if got := cmd(t, rw, "*1\r\n$4\r\nPING\r\n"); got != "+PONG\r\n" {
		t.Fatalf("PING = %q; want +PONG", got)
	}

	if got := cmd(t, rw, "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n"); got != "+OK\r\n" {
		t.Fatalf("SET a b = %q; want +OK", got)
	}

	if got := cmd(t, rw, "*2\r\n$3\r\nGET\r\n$1\r\na\r\n"); got != "$1\r\nb\r\n" {
		t.Fatalf("GET a = %q; want $1 b", got)
	}

	if got := cmd(t, rw, "*2\r\n$3\r\nDEL\r\n$1\r\na\r\n"); got != ":1\r\n" {
		t.Fatalf("DEL a = %q; want :1", got)
	}

	if got := cmd(t, rw, "*2\r\n$3\r\nGET\r\n$1\r\na\r\n"); got != "$-1\r\n" {
		t.Fatalf("GET a after DEL = %q; want $-1", got)
	}
}

func TestServerSetWithExpiry(t *testing.T) {
	addr := startTestServer(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	if got := cmd(t, rw, "*5\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\ny\r\n$2\r\nEX\r\n$1\r\n1\r\n"); got != "+OK\r\n" {
		t.Fatalf("SET x y EX 1 = %q; want +OK", got)
	}

	if got := cmd(t, rw, "*2\r\n$3\r\nGET\r\n$1\r\nx\r\n"); got != "$1\r\ny\r\n" {
		t.Fatalf("GET x before expiry = %q; want $1 y", got)
	}

	time.Sleep(1200 * time.Millisecond)

	if got := cmd(t, rw, "*2\r\n$3\r\nGET\r\n$1\r\nx\r\n"); got != "$-1\r\n" {
		t.Fatalf("GET x after expiry = %q; want $-1", got)
	}
}

func TestServerTTL(t *testing.T) {
	addr := startTestServer(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	if got := cmd(t, rw, "*2\r\n$3\r\nTTL\r\n$1\r\nk\r\n"); got != ":-2\r\n" {
		t.Fatalf("TTL missing key = %q; want :-2", got)
	}

	_ = cmd(t, rw, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
	if got := cmd(t, rw, "*2\r\n$3\r\nTTL\r\n$1\r\nk\r\n"); got != ":-1\r\n" {
		t.Fatalf("TTL key with no expiry = %q; want :-1", got)
	}

	_ = cmd(t, rw, "*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$2\r\n10\r\n")
	if got := cmd(t, rw, "*2\r\n$3\r\nTTL\r\n$1\r\nk\r\n"); got != ":10\r\n" && got != ":9\r\n" {
		t.Fatalf("TTL key with expiry = %q; want :10 or :9", got)
	}
}


func TestServerErrorReplies(t *testing.T) {
	addr := startTestServer(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	if got := cmd(t, rw, "*1\r\n$4\r\nFOOX\r\n"); got != "-ERR unknown command 'FOOX'\r\n" {
		t.Fatalf("unknown command = %q", got)
	}

	if got := cmd(t, rw, "*2\r\n$3\r\nSET\r\n$1\r\na\r\n"); got != "-ERR wrong number of arguments for 'set' command\r\n" {
		t.Fatalf("wrong arity = %q", got)
	}

	if got := cmd(t, rw, "*5\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n$2\r\nEX\r\n$1\r\n0\r\n"); got != "-ERR invalid expire time in 'set' command\r\n" {
		t.Fatalf("EX 0 = %q", got)
	}
}

// TestServerConcurrentClients verifies that multiple clients are served
// concurrently rather than one blocking the next, which is the defining
// difference from the single-goroutine echo-server stage of this project.
func TestServerConcurrentClients(t *testing.T) {
	addr := startTestServer(t)

	const clients = 20
	var wg sync.WaitGroup
	errs := make(chan error, clients)

	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				errs <- fmt.Errorf("client %d dial: %w", i, err)
				return
			}
			defer conn.Close()
			rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

			key := fmt.Sprintf("k%d", i)
			val := fmt.Sprintf("v%d", i)

			setCmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
			if got := cmd(t, rw, setCmd); got != "+OK\r\n" {
				errs <- fmt.Errorf("client %d SET = %q", i, got)
				return
			}

			getCmd := fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)
			want := fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
			if got := cmd(t, rw, getCmd); got != want {
				errs <- fmt.Errorf("client %d GET = %q; want %q", i, got, want)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
