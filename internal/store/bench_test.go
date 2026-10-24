package store

import (
	"strconv"
	"testing"
)

func BenchmarkSet(b *testing.B) {
	st, err := Open(b.TempDir(), "bench")
	if err != nil {
		b.Fatal(err)
	}
	defer st.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := st.Set("doc", "field", strconv.Itoa(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMissing(b *testing.B) {
	st, err := Open(b.TempDir(), "bench")
	if err != nil {
		b.Fatal(err)
	}
	defer st.Close()
	for i := 0; i < 1000; i++ {
		if _, err := st.Set("doc", "field", strconv.Itoa(i)); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := len(st.Missing(map[string]uint64{"bench": 500})); got != 500 {
			b.Fatalf("missing = %d, want 500", got)
		}
	}
}
