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

func TestCLILiteralValues(t *testing.T) {
	for _, value := range []string{"help", "--help", "-1", "-", "--"} {
		t.Run(value, func(t *testing.T) {
			dir := t.TempDir()
			args := []string{"--data", dir, "--node", "a", "put", "d", "f"}
			if strings.HasPrefix(value, "-") {
				args = append(args, "--")
			}
			args = append(args, value)
			out, _, err := runCLI(t, args...)
			if err != nil || !strings.HasPrefix(out, "ok a/") {
				t.Fatalf("put out=%q err=%v", out, err)
			}
			out, _, err = runCLI(t, "--data", dir, "--node", "a", "get", "d")
			if err != nil || !strings.Contains(out, `"value": "`+value+`"`) {
				t.Fatalf("get out=%q err=%v", out, err)
			}
		})
	}
}

func TestCLIUsageDoesNotCreateStore(t *testing.T) {
	for _, args := range [][]string{
		{"put", "d", "f"}, {"get"}, {"ls", "extra"}, {"status", "extra"},
		{"serve", "extra"}, {"sync"}, {"export", "a", "b"}, {"import"},
		{"log", "1", "2"}, {"log", "invalid"}, {"--unknown"},
	} {
		dir := filepath.Join(t.TempDir(), "not-created")
		all := append([]string{"--data", dir, "--node", "a"}, args...)
		_, stderr, err := runCLI(t, all...)
		if !errors.Is(err, errUsage) || !strings.Contains(stderr, "driftdb:") {
			t.Errorf("args=%v stderr=%q err=%v", args, stderr, err)
		}
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Errorf("invalid command %v created a store", args)
		}
	}
}

func TestExportCannotOverwriteStoreFiles(t *testing.T) {
	for _, name := range []string{"node", "ops.jsonl"} {
		for _, alias := range []string{"direct", "symlink", "hardlink"} {
			t.Run(name+"/"+alias, func(t *testing.T) {
				dir := t.TempDir()
				if _, _, err := runCLI(t, "--data", dir, "--node", "a", "put", "d", "f", "v"); err != nil {
					t.Fatal(err)
				}
				original := filepath.Join(dir, name)
				before, err := os.ReadFile(original)
				if err != nil {
					t.Fatal(err)
				}
				target := original
				if alias != "direct" {
					target = filepath.Join(t.TempDir(), "alias")
					link := os.Link
					if alias == "symlink" {
						link = os.Symlink
					}
					if err := link(original, target); err != nil {
						t.Fatal(err)
					}
				}
				if _, _, err := runCLI(t, "--data", dir, "--node", "a", "export", target); err == nil {
					t.Error("export accepted a live store file")
				}
				after, err := os.ReadFile(original)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(before, after) {
					t.Error("export corrupted store file")
				}
			})
		}
	}
}

func TestExportReplacesBackupAndCleansTemporaryFiles(t *testing.T) {
	dir := t.TempDir()
	if _, _, err := runCLI(t, "--data", dir, "--node", "a", "put", "d", "f", "new"); err != nil {
		t.Fatal(err)
	}
	backupDir := t.TempDir()
	target := filepath.Join(backupDir, "backup.jsonl")
	if err := os.WriteFile(target, []byte("old backup"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runCLI(t, "--data", dir, "--node", "a", "export", target); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"value":"new"`)) || bytes.Contains(raw, []byte("old backup")) {
		t.Fatalf("backup=%s", raw)
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatal("changed backup permissions")
	}
	// A nonempty directory cannot be replaced. The error must leave both the
	// destination and its contents intact, without an orphaned temporary file.
	blocked := filepath.Join(backupDir, "blocked")
	if err := os.Mkdir(blocked, 0o755); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(blocked, "keep")
	if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runCLI(t, "--data", dir, "--node", "a", "export", blocked); err == nil {
		t.Fatal("expected export failure")
	}
	if got, err := os.ReadFile(sentinel); err != nil || string(got) != "keep" {
		t.Fatalf("destination changed: %q %v", got, err)
	}
	leftovers, err := filepath.Glob(filepath.Join(backupDir, ".driftdb-export-*"))
	if err != nil || len(leftovers) != 0 {
		t.Fatalf("temporary files=%v err=%v", leftovers, err)
	}
}
