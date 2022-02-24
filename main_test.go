package main

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"path/filepath"
	"testing"
)

type TestLog interface {
	Fatal(args ...interface{})
	Cleanup(func())
}

func TestSerialParse(t *testing.T) {
	CreateTestFiles(t, 10)
	term := "create"
	res, err := SerialParse("testdata", term, 1024)
	require.NoError(t, err, "something went wrong")

	verifyParseResults(t, res, term)
}

func TestParallelParse(t *testing.T) {
	CreateTestFiles(t, 10)
	term := "create"
	res, err := ParallelParse("testdata", term, 1024, 10)
	require.NoError(t, err, "something went wrong")

	verifyParseResults(t, res, term)
}

func benchmarkSerialParse(n int, b *testing.B) {
	CreateTestFiles(b, n)
	for i := 0; i < b.N; i++ {
		_, err := SerialParse("testdata", "create", 1024)
		require.NoError(b, err)
	}
}

func BenchmarkSerialParse10(b *testing.B)   { benchmarkSerialParse(10, b) }
func BenchmarkSerialParse100(b *testing.B)  { benchmarkSerialParse(100, b) }
func BenchmarkSerialParse1000(b *testing.B) { benchmarkSerialParse(1000, b) }

func benchmarkParallelParse(n int, b *testing.B) {
	CreateTestFiles(b, n)
	for i := 0; i < b.N; i++ {
		_, err := ParallelParse("testdata", "create", 1024, 10)
		require.NoError(b, err)
	}
}

func BenchmarkParallelParse10(b *testing.B)   { benchmarkParallelParse(10, b) }
func BenchmarkParallelParse100(b *testing.B)  { benchmarkParallelParse(100, b) }
func BenchmarkParallelParse1000(b *testing.B) { benchmarkParallelParse(1000, b) }

func verifyParseResults(t *testing.T, res ParseResult, term string) {
	for filename, offsets := range res {
		func() {
			file, err := os.Open(filename)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err = file.Close(); err != nil {
					t.Fatal(err)
				}
			}()

			for _, offset := range offsets {
				actual := make([]byte, len(term))
				_, err = file.ReadAt(actual, offset)
				require.Equal(t, []byte(term), actual)
			}
		}()
	}
}

func CreateTestFiles(t TestLog, n int) {
	filename := "example.log"
	source, err := os.Open("testdata/example.log")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = source.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	for i := 0; i < n; i++ {
		// wrap to ensure defer executes asap see:
		// https://stackoverflow.com/a/45620423
		func() {
			name := filepath.Join("testdata", fmt.Sprintf("%d-%s", i, filename))
			var f *os.File
			f, err = os.Create(name)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err = f.Close(); err != nil {
					t.Fatal(err)
				}
			}()

			if _, err = io.Copy(f, source); err != nil {
				t.Fatal(err)
			}
		}()
	}

	t.Cleanup(func() {
		CleanupTestFiles(t)
	})
}

func CleanupTestFiles(t TestLog) {
	if err := filepath.WalkDir("testdata", func(path string, dir os.DirEntry, err error) error {
		if dir.Name() == "example.log" || dir.IsDir() {
			return nil
		} else {
			if err = os.Remove(path); err != nil {
				return err
			}
			return nil
		}
	}); err != nil {
		t.Fatal(err)
	}
}
