package main

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"log"
	"os"
	"path/filepath"
)

func main() {
	err := filepath.WalkDir("testdata", func(path string, dir os.DirEntry, err error) error {
		fmt.Println(path, dir.Name())
		fmt.Println(err)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}

type ParseResult map[string][]int64

func SerialParse(dir string, term string, bufSize int) (ParseResult, error) {
	res := ParseResult{}
	err := filepath.WalkDir(dir, func(path string, dir os.DirEntry, err error) error {
		if dir.IsDir() {
			return nil
		}
		return processFile(path, term, bufSize, res)
	})
	return res, err
}

// Parallel version walks a dir to get files to parse and pushes them to chan skipping dirs
// workers pull from the filenames and do the parsing
func ParallelParse(dir string, term string, bufSize int, workerCount int) (ParseResult, error) {
	res := ParseResult{}
	files := make(chan string)
	go func() {
		if err := collectFiles(dir, files); err != nil {
			log.Fatal(err)
		}
		close(files)
	}()

	errorChan := make(chan error)
	defer close(errorChan)
	var processErr error

WorkLoop:
	for {
		select {
		case f, ok := <-files:
			if !ok {
				files = nil
				break WorkLoop
			}
			go func() {
				if err := processFile(f, term, bufSize, res); err != nil {
					errorChan <- err
				}
			}()

		case err, ok := <-errorChan:
			if !ok {
				errorChan = nil
				break WorkLoop
			}
			processErr = errors.Wrap(processErr, err.Error())
		}
	}
	return res, processErr
}

func collectFiles(dir string, out chan<- string) error {
	return filepath.WalkDir(dir, func(path string, dir os.DirEntry, err error) error {
		if dir.IsDir() {
			return nil
		}
		out <- path
		return nil
	})
}

func processFile(path string, term string, bufSize int) (string, []int64, error) {
	var positions []int64

	var f *os.File
	f, err := os.Open(path)
	if err != nil {
		return path, positions, err
	}
	defer func() {
		err = f.Close()
	}()

	termBytes := []byte(term)
	var offset int
	b := make([]byte, bufSize)
	for {
		var n int
		n, err = f.Read(b)
		if err != nil {
			if err == io.EOF {
				// reset error for return - EOF is good
				err = nil
				break
			}
			break
		}

		pos := bytes.Index(b, termBytes)
		if pos != -1 {
			if err != nil {
				break
			}
			positions = append(positions, int64(pos))
		}
		offset += n
	}

	return path, positions, err
}
