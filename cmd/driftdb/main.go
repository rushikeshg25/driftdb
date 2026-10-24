// Command driftdb is the CLI for a local-first document store that replicates
// with anti-entropy over TCP.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rushikeshg25/driftdb/internal/crdt"
	"github.com/rushikeshg25/driftdb/internal/store"
	"github.com/rushikeshg25/driftdb/internal/syncer"
)

const usage = `driftdb - a local-first document store

usage:
  driftdb [flags] <command> [arguments]

commands:
  get <doc>                  print a document as JSON
  put <doc> <field> <value>  set a register field
  del <doc> <field>          delete a register field
  sadd <doc> <field> <elem>  add an element to a set field
  srem <doc> <field> <elem>  remove an element from a set field
  ls                         list documents
  log [limit]                show the operation log (newest last)
  status                     show replica statistics
  export [path]              write the op log as NDJSON
  import <path>              merge an NDJSON op log into this replica
  sync <address>             exchange operations with a peer once
  serve                      serve replication until interrupted

flags (may appear before or after the command):
  --data DIR     data directory (default ./driftdb-data)
  --node ID      replica id (default $DRIFTDB_NODE or "local")
  --listen ADDR  replication listen address (default 127.0.0.1:7070)
  --             treat remaining arguments as literal values
`

type options struct {
	data   string
	node   string
	listen string
}

var errUsage = errors.New("usage")

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, errUsage) {
			os.Exit(2)
		}
		fmt.Fprintln(os.Stderr, "driftdb:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	opts, rest, err := parseArgs(args)
	if err != nil {
		fmt.Fprintf(stderr, "driftdb: %v\n\n%s", err, usage)
		return errUsage
	}
	if len(rest) == 0 || rest[0] == "help" {
		fmt.Fprint(stdout, usage)
		return nil
	}

	cmd, args := rest[0], rest[1:]
	if err := validateCommandArgs(cmd, args); err != nil {
		fmt.Fprintf(stderr, "driftdb: %v\n\n%s", err, usage)
		return errUsage
	}
	switch cmd {
	case "get", "put", "del", "sadd", "srem", "ls", "log", "status", "export", "import":
		st, err := store.Open(opts.data, opts.node)
		if err != nil {
			return err
		}
		defer st.Close()
		return runLocal(st, cmd, args, stdout)
	case "sync":
		st, err := store.Open(opts.data, opts.node)
		if err != nil {
			return err
		}
		defer st.Close()
		return runSync(st, args, stdout)
	case "serve":
		return runServe(opts, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "driftdb: unknown command %q\n\n", cmd)
		fmt.Fprint(stderr, usage)
		return errUsage
	}
}

// Validate before opening the store so malformed commands have no disk or
// network side effects.
func validateCommandArgs(cmd string, args []string) error {
	minArgs, maxArgs := 0, 0
	switch cmd {
	case "get", "import", "sync":
		minArgs, maxArgs = 1, 1
	case "put", "sadd", "srem":
		minArgs, maxArgs = 3, 3
	case "del":
		minArgs, maxArgs = 2, 2
	case "log", "export":
		maxArgs = 1
	case "ls", "status", "serve":
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
	if len(args) < minArgs || len(args) > maxArgs {
		return fmt.Errorf("%s expects %d-%d arguments, got %d", cmd, minArgs, maxArgs, len(args))
	}
	if cmd == "log" && len(args) == 1 {
		if n, err := strconv.Atoi(args[0]); err != nil || n < 0 {
			return fmt.Errorf("invalid log limit %q", args[0])
		}
	}
	return nil
}

func parseArgs(args []string) (options, []string, error) {
	opts := options{
		data:   envOr("DRIFTDB_DATA", "./driftdb-data"),
		node:   envOr("DRIFTDB_NODE", "local"),
		listen: envOr("DRIFTDB_LISTEN", "127.0.0.1:7070"),
	}
	var rest []string

	set := func(name, value string) error {
		switch name {
		case "data":
			opts.data = value
		case "node":
			opts.node = value
		case "listen":
			opts.listen = value
		default:
			return fmt.Errorf("unknown flag --%s", name)
		}
		return nil
	}

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			rest = append(rest, args[i+1:]...)
			break
		}
		if arg == "-h" || arg == "--help" {
			return opts, nil, nil
		}
		if !strings.HasPrefix(arg, "-") || arg == "-" {
			rest = append(rest, arg)
			continue
		}
		name := strings.TrimLeft(arg, "-")
		if eq := strings.IndexByte(name, '='); eq >= 0 {
			if err := set(name[:eq], name[eq+1:]); err != nil {
				return opts, nil, err
			}
			continue
		}
		if i+1 >= len(args) {
			return opts, nil, fmt.Errorf("flag --%s needs a value", name)
		}
		i++
		if err := set(name, args[i]); err != nil {
			return opts, nil, err
		}
	}
	return opts, rest, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func runLocal(st *store.Store, cmd string, args []string, out io.Writer) error {
	switch cmd {
	case "get":
		if len(args) != 1 {
			return fmt.Errorf("get needs exactly one document id")
		}
		fields, ok := st.Doc(args[0])
		if !ok {
			return fmt.Errorf("document %q not found", args[0])
		}
		return printJSON(out, fields)
	case "put":
		if len(args) != 3 {
			return fmt.Errorf("put needs <doc> <field> <value>")
		}
		op, err := st.Set(args[0], args[1], args[2])
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "ok %s\n", op.ID())
		return nil
	case "del":
		if len(args) != 2 {
			return fmt.Errorf("del needs <doc> <field>")
		}
		op, err := st.Delete(args[0], args[1])
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "ok %s\n", op.ID())
		return nil
	case "sadd":
		if len(args) != 3 {
			return fmt.Errorf("sadd needs <doc> <field> <element>")
		}
		op, err := st.SetAdd(args[0], args[1], args[2])
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "ok %s\n", op.ID())
		return nil
	case "srem":
		if len(args) != 3 {
			return fmt.Errorf("srem needs <doc> <field> <element>")
		}
		op, err := st.SetRemove(args[0], args[1], args[2])
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "ok %s\n", op.ID())
		return nil
	case "ls":
		for _, id := range st.DocIDs() {
			fmt.Fprintln(out, id)
		}
		return nil
	case "log":
		return printLog(st, args, out)
	case "status":
		return printJSON(out, st.Stats())
	case "export":
		return exportOps(st, args, out)
	case "import":
		return importOps(st, args, out)
	default:
		return fmt.Errorf("unhandled command %q", cmd)
	}
}

func printLog(st *store.Store, args []string, out io.Writer) error {
	limit := 0
	if len(args) > 1 {
		return fmt.Errorf("log takes at most one limit argument")
	}
	if len(args) == 1 {
		n, err := strconv.Atoi(args[0])
		if err != nil || n < 0 {
			return fmt.Errorf("invalid log limit %q", args[0])
		}
		limit = n
	}

	ops := st.Ops()
	if limit > 0 && len(ops) > limit {
		ops = ops[len(ops)-limit:]
	}
	for _, op := range ops {
		ts := time.UnixMilli(op.HLC.Wall).UTC().Format("2006-01-02T15:04:05.000Z")
		fmt.Fprintf(out, "%s  %-4s %-10s %s.%s",
			ts, op.Kind, op.Node+"/"+strconv.FormatUint(op.Seq, 10), op.Doc, op.Field)
		switch op.Kind {
		case crdt.OpSet:
			fmt.Fprintf(out, " = %s", op.Value)
		case crdt.OpDelete:
			fmt.Fprint(out, " (tombstone)")
		case crdt.OpSetAdd:
			fmt.Fprintf(out, " += %s", op.Element)
		case crdt.OpSetRemove:
			fmt.Fprintf(out, " -= %s", op.Element)
		}
		fmt.Fprintln(out)
	}
	return nil
}

func exportOps(st *store.Store, args []string, out io.Writer) error {
	if len(args) > 1 {
		return fmt.Errorf("export takes at most one path")
	}
	ops := st.Ops()
	if len(args) == 0 {
		return writeOps(out, ops)
	}
	target := args[0]
	mode := os.FileMode(0o644)
	info, err := os.Stat(target)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err == nil {
		for _, name := range []string{"node", "ops.jsonl"} {
			protected, err := os.Stat(filepath.Join(st.Dir(), name))
			if err != nil {
				return err
			}
			if os.SameFile(info, protected) {
				return fmt.Errorf("export cannot overwrite store file %s", name)
			}
		}
		mode = info.Mode().Perm()
	}
	// Do not truncate an existing backup until the complete export is ready.
	// Create the temporary file beside the destination so rename stays on one
	// filesystem. Rename replaces an ordinary destination without following it.
	f, err := os.CreateTemp(filepath.Dir(target), ".driftdb-export-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if err := writeOps(f, ops); err != nil {
		return err
	}
	if err := f.Chmod(mode); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), target)
}

func writeOps(out io.Writer, ops []crdt.Op) error {
	enc := json.NewEncoder(out)
	for _, op := range ops {
		if err := enc.Encode(op); err != nil {
			return err
		}
	}
	return nil
}

func importOps(st *store.Store, args []string, out io.Writer) error {
	if len(args) != 1 {
		return fmt.Errorf("import needs a path")
	}
	f, err := os.Open(args[0])
	if err != nil {
		return err
	}
	defer f.Close()

	var ops []crdt.Op
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 8<<20)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var op crdt.Op
		if err := json.Unmarshal(line, &op); err != nil {
			return fmt.Errorf("decode line: %w", err)
		}
		ops = append(ops, op)
	}
	if err := sc.Err(); err != nil {
		return err
	}
	n, err := st.Apply(ops)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "imported %d ops\n", n)
	return nil
}

func printJSON(out io.Writer, v any) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func runSync(st *store.Store, args []string, out io.Writer) error {
	if len(args) != 1 {
		return fmt.Errorf("sync needs a peer address")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pulled, pushed, err := syncer.Exchange(ctx, args[0], st)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "pulled %d, pushed %d\n", pulled, pushed)
	return nil
}

func runServe(opts options, stdout, stderr io.Writer) error {
	st, err := store.Open(opts.data, opts.node)
	if err != nil {
		return err
	}
	defer st.Close()

	ln, err := net.Listen("tcp", opts.listen)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "driftdb %s listening on %s (data: %s)\n", st.Node(), ln.Addr(), st.Dir())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// Serve closes the listener and active connections when ctx is canceled.
	return syncer.Serve(ctx, ln, st)
}
