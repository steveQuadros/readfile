package main

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

type TestLog interface {
	Fatal(args ...interface{})
	Cleanup(func())
}

func TestSerialParse(t *testing.T) {
	fileCount := 10
	dir := CreateTestFiles(t, fileCount)
	term := "create"
	res, err := SerialParse(dir, term, 1024)
	require.NoError(t, err, "something went wrong")
	require.NotEmpty(t, res)
	require.Len(t, res, fileCount)
	verifyParseResults(t, res, term)
}

func TestParallelParse(t *testing.T) {
	term := "create"
	_, err := ParallelParse("shoulderrordirnotexist", term, 1024, 10)
	require.Error(t, err)

	fileCount := 10
	dir := CreateTestFiles(t, fileCount)
	res, err := ParallelParse(dir, term, 1024, 10)
	require.NoError(t, err)
	require.Len(t, res, fileCount)
	verifyParseResults(t, res, term)
}

func benchmarkSerialParse(n int, b *testing.B) {
	dir := CreateTestFiles(b, n)
	for i := 0; i < b.N; i++ {
		_, err := SerialParse(dir, "create", 1024)
		require.NoError(b, err)
	}
}

func BenchmarkSerialParse10(b *testing.B) { benchmarkSerialParse(10, b) }

func BenchmarkSerialParse100(b *testing.B) { benchmarkSerialParse(100, b) }

//func BenchmarkSerialParse1000(b *testing.B) { benchmarkSerialParse(1000, b) }

func benchmarkParallelParse(n int, w int, b *testing.B) {
	dir := CreateTestFiles(b, n)
	for i := 0; i < b.N; i++ {
		_, err := ParallelParse(dir, "create", 1024, w)
		require.NoError(b, err)
	}
}

func BenchmarkParallelParse10(b *testing.B) { benchmarkParallelParse(10, 10, b) }

func BenchmarkParallelParse100(b *testing.B) { benchmarkParallelParse(100, 10, b) }

//func BenchmarkParallelParse1000(b *testing.B) { benchmarkParallelParse(1000, 100, b) }

func verifyParseResults(t *testing.T, res []ParseResult, term string) {
	for _, r := range res {
		func() {
			file, err := os.Open(r.path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err = file.Close(); err != nil {
					t.Fatal(err)
				}
			}()

			for _, offset := range r.offsets {
				actual := make([]byte, len(term))
				_, err = file.ReadAt(actual, offset)
				require.Equal(t, []byte(term), actual)
			}
		}()
	}
}

// @TODO create unique dir for test set to use it's own files to avoid issues

func CreateTestFiles(t TestLog, n int) string {
	dest, err := ioutil.TempDir("", "")

	var cleanup func(error)
	cleanup = func(err error) {
		if err != nil {
			CleanupTestFiles(t, dest)
			t.Fatal(err)
		}
	}

	filename := "example.log"
	source, err := os.Open("testdata/example.log")
	cleanup(err)

	defer func() {
		if err = source.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	cleanup(err)

	for i := 0; i < n; i++ {
		// wrap to ensure defer executes asap see:
		// https://stackoverflow.com/a/45620423
		func() {
			name := filepath.Join(dest, fmt.Sprintf("%d-%s", i, filename))
			var f *os.File
			f, err = os.Create(name)
			cleanup(err)
			defer func() {
				cleanup(f.Close())
			}()

			_, err = io.Copy(f, source)
			cleanup(err)
		}()
	}

	t.Cleanup(func() {
		CleanupTestFiles(t, dest)
	})
	return dest
}

func CleanupTestFiles(t TestLog, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}
