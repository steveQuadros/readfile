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
	res, err := SerialParse("testdata", "create", 1024)
	require.NoError(t, err, "something went wrong")
	require.Equal(t, res, map[string]int64{})
}

func BenchmarkSerialParse(b *testing.B) {
	CreateTestFiles(b, 10)
	for i := 0; i < b.N; i++ {
		_, err := SerialParse("testdata", "create", 1024)
		require.NoError(b, err)
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
		// wrap to ensure defer exectutes asap see:
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
