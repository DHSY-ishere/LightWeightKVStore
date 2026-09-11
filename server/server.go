// Package server implements the TCP front-end: it accepts client
// connections, decodes RESP commands off the wire, dispatches them
// against the store, and encodes RESP replies back.
package server

import (
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"kvstore/resp"
	"kvstore/store"
)

// Server owns the listener, the shared storage engine, and the set of
// currently active client connections (needed so Shutdown can close
// them out from under any goroutines blocked reading on them).
type Server struct {
	addr string
	db   *store.Store

	listener net.Listener

	connsMu sync.Mutex
	conns   map[net.Conn]struct{}
	wg      sync.WaitGroup

	// shuttingDown is set right before the listener and connections are
	// closed, so the accept loop and connection handlers can tell a
	// "connection closed" error apart from a genuine failure and skip
	// logging noise during an intentional shutdown.
	shuttingDown int32
}

func New(addr string, db *store.Store) *Server {
	return &Server{
		addr:  addr,
		db:    db,
		conns: make(map[net.Conn]struct{}),
	}
}

// ListenAndServe binds the TCP listener and accepts connections in a
// loop, handing each one off to its own goroutine so slow or idle
// clients never block other clients. It returns nil if the listener
// was closed by Shutdown, or the underlying error otherwise.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = ln
	defer ln.Close()

	log.Printf("kvstore listening on %s", s.addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if atomic.LoadInt32(&s.shuttingDown) == 1 {
				return nil
			}
			log.Printf("accept error: %v", err)
			continue
		}

		s.trackConn(conn)
		s.wg.Add(1)
		// One goroutine per connection: the store's per-shard locking
		// (see store.Store) is what makes it safe for many of these
		// to read/write concurrently without a global bottleneck.
		go func() {
			defer s.wg.Done()
			defer s.untrackConn(conn)
			s.handleConn(conn)
		}()
	}
}

// Shutdown stops the listener (so no new connections are accepted),
// force-closes every currently open connection (which unblocks any
// goroutine parked in a read on one of them), and waits for all
// connection-handling goroutines to return. It does not touch the
// store, which is purely in-memory and has nothing to flush.
func (s *Server) Shutdown() {
	atomic.StoreInt32(&s.shuttingDown, 1)

	if s.listener != nil {
		_ = s.listener.Close()
	}

	s.connsMu.Lock()
	for c := range s.conns {
		_ = c.Close()
	}
	s.connsMu.Unlock()

	s.wg.Wait()
}

func (s *Server) trackConn(c net.Conn) {
	s.connsMu.Lock()
	s.conns[c] = struct{}{}
	s.connsMu.Unlock()
}

func (s *Server) untrackConn(c net.Conn) {
	s.connsMu.Lock()
	delete(s.conns, c)
	s.connsMu.Unlock()
}

// handleConn services a single client connection until it disconnects,
// sends a malformed frame, or is force-closed by Shutdown.
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)

	for {
		req, err := reader.Read()
		if err != nil {
			if !errors.Is(err, io.EOF) && atomic.LoadInt32(&s.shuttingDown) == 0 {
				log.Printf("read error from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}

		args, err := toArgs(req)
		if err != nil {
			_ = writer.WriteError("ERR " + err.Error())
			continue
		}
		if len(args) == 0 {
			_ = writer.WriteError("ERR empty command")
			continue
		}

		s.dispatch(writer, args)
	}
}

// toArgs flattens a RESP request into a slice of string arguments.
// Real clients (redis-cli, redis-py, etc.) send commands as a RESP
// Array of Bulk Strings, e.g. *3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n.
// Inline commands typed as a single line (a bare SimpleString) are
// also accepted for convenience when testing with raw netcat/telnet.
func toArgs(v resp.Value) ([]string, error) {
	switch v.Type {
	case resp.Array:
		args := make([]string, 0, len(v.Elems))
		for _, e := range v.Elems {
			if e.Type != resp.BulkString || e.IsNull {
				return nil, errors.New("expected bulk string array element")
			}
			args = append(args, e.Str)
		}
		return args, nil
	case resp.SimpleString:
		return strings.Fields(v.Str), nil
	default:
		return nil, errors.New("unsupported request type")
	}
}

// dispatch routes a decoded command to its handler and writes the
// RESP reply. Command names are case-insensitive per the Redis spec.
func (s *Server) dispatch(w *resp.Writer, args []string) {
	cmd := strings.ToUpper(args[0])

	switch cmd {
	case "PING":
		s.handlePing(w, args)
	case "SET":
		s.handleSet(w, args)
	case "GET":
		s.handleGet(w, args)
	case "DEL":
		s.handleDel(w, args)
	default:
		_ = w.WriteError("ERR unknown command '" + args[0] + "'")
	}
}

func (s *Server) handlePing(w *resp.Writer, args []string) {
	if len(args) > 1 {
		_ = w.WriteBulkString(args[1])
		return
	}
	_ = w.WriteSimpleString("PONG")
}

// handleSet implements SET key value [EX seconds].
func (s *Server) handleSet(w *resp.Writer, args []string) {
	if len(args) != 3 && len(args) != 5 {
		_ = w.WriteError("ERR wrong number of arguments for 'set' command")
		return
	}

	key, value := args[1], args[2]
	var ttl time.Duration

	if len(args) == 5 {
		if strings.ToUpper(args[3]) != "EX" {
			_ = w.WriteError("ERR syntax error")
			return
		}
		secs, err := strconv.ParseInt(args[4], 10, 64)
		if err != nil || secs <= 0 {
			_ = w.WriteError("ERR invalid expire time in 'set' command")
			return
		}
		ttl = time.Duration(secs) * time.Second
	}

	s.db.Set(key, value, ttl)
	_ = w.WriteSimpleString("OK")
}

func (s *Server) handleGet(w *resp.Writer, args []string) {
	if len(args) != 2 {
		_ = w.WriteError("ERR wrong number of arguments for 'get' command")
		return
	}
	// store.Get already applies lazy expiration: an expired key is
	// deleted on this read and reported as a miss.
	value, ok := s.db.Get(args[1])
	if !ok {
		_ = w.WriteNullBulk()
		return
	}
	_ = w.WriteBulkString(value)
}

func (s *Server) handleDel(w *resp.Writer, args []string) {
	if len(args) < 2 {
		_ = w.WriteError("ERR wrong number of arguments for 'del' command")
		return
	}
	var deleted int64
	for _, key := range args[1:] {
		deleted += int64(s.db.Del(key))
	}
	_ = w.WriteInteger(deleted)
}
