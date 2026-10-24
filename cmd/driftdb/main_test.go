package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func runCLI(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	err := run(args, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

func TestParseArgsAcceptsFlagsBeforeAndAfterCommand(t *testing.T) {
	before, rest, err := parseArgs([]string{"--data", "/tmp/x", "put", "d", "f", "v", "--node=n1"})
	if err != nil {
		t.Fatal(err)
	}
	if before.data != "/tmp/x" || before.node != "n1" {
		t.Fatalf("opts = %+v", before)
	}
	if strings.Join(rest, " ") != "put d f v" {
		t.Fatalf("rest = %v", rest)
	}

	after, _, err := parseArgs([]string{"get", "doc", "--listen", "127.0.0.1:9999"})
	if err != nil {
		t.Fatal(err)
	}
	if after.listen != "127.0.0.1:9999" {
		t.Fatalf("listen = %q", after.listen)
	}
}

func TestParseArgsRejectsMissingValue(t *testing.T) {
	if _, _, err := parseArgs([]string{"ls", "--data"}); err == nil {
		t.Fatal("expected an error")
	}
}

func TestCLIRoundTrip(t *testing.T) {
	dir := t.TempDir()
	if out, _, err := runCLI(t, "--data", dir, "--node", "a", "put", "note", "title", "hello"); err != nil {
		t.Fatal(err)
	} else if !strings.HasPrefix(out, "ok a/") {
		t.Fatalf("put output = %q", out)
	}

	if _, _, err := runCLI(t, "--data", dir, "--node", "a", "sadd", "note", "tags", "urgent"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runCLI(t, "--data", dir, "--node", "a", "sadd", "note", "tags", "later"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runCLI(t, "--data", dir, "--node", "a", "srem", "note", "tags", "urgent"); err != nil {
		t.Fatal(err)
	}

	out, _, err := runCLI(t, "--data", dir, "--node", "a", "get", "note")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `"hello"`) || !strings.Contains(out, `"later"`) || strings.Contains(out, "urgent") {
		t.Fatalf("unexpected get output:\n%s", out)
	}

	out, _, err = runCLI(t, "--data", dir, "--node", "a", "ls")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(out) != "note" {
		t.Fatalf("ls = %q", out)
	}

	out, _, err = runCLI(t, "--data", dir, "--node", "a", "log", "2")
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(out, "\n"); lines != 2 {
		t.Fatalf("log lines = %d, want 2:\n%s", lines, out)
	}
}

func TestCLIReportsMissingDocument(t *testing.T) {
	_, _, err := runCLI(t, "--data", t.TempDir(), "--node", "a", "get", "nope")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("err = %v", err)
	}
}

func TestCLIExportImport(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	if _, _, err := runCLI(t, "--data", src, "--node", "a", "put", "note", "title", "shared"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runCLI(t, "--data", src, "--node", "a", "sadd", "note", "tags", "x"); err != nil {
		t.Fatal(err)
	}

	exportPath := filepath.Join(t.TempDir(), "ops.jsonl")
	if _, _, err := runCLI(t, "--data", src, "--node", "a", "export", exportPath); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(exportPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatal("export file is empty")
	}

	out, _, err := runCLI(t, "--data", dst, "--node", "b", "import", exportPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(out) != "imported 2 ops" {
		t.Fatalf("import output = %q", out)
	}

	got, _, err := runCLI(t, "--data", dst, "--node", "b", "get", "note")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, `"shared"`) {
		t.Fatalf("imported doc missing value:\n%s", got)
	}
}

func TestCLIUnknownCommand(t *testing.T) {
	_, stderr, err := runCLI(t, "bogus")
	if !errors.Is(err, errUsage) {
		t.Fatalf("err = %v, want errUsage", err)
	}
	if !strings.Contains(stderr, "unknown command") {
		t.Fatalf("stderr = %q", stderr)
	}
}

func TestCLIHelp(t *testing.T) {
	out, _, err := runCLI(t, "--help")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "driftdb") || !strings.Contains(out, "serve") {
		t.Fatalf("unexpected help:\n%s", out)
	}
}
