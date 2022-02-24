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

type ParseResult struct {
	path    string
	offsets []int64
}

func SerialParse(dir string, term string, bufSize int) ([]ParseResult, error) {
	res := []ParseResult{}
	err := filepath.WalkDir(dir, func(path string, dir os.DirEntry, err error) error {
		if dir.IsDir() {
			return nil
		}
		pr, err := processFile(path, term, bufSize)
		if err != nil {
			return err
		}
		res = append(res, pr)
		return nil
	})
	return res, err
}

// Parallel version walks a dir to get files to parse and pushes them to chan skipping dirs
// workers pull from the filenames and do the parsing
func ParallelParse(dir string, term string, bufSize int, workerCount int) ([]ParseResult, error) {
	res := []ParseResult{}
	files := make(chan string)
	go func() {
		if err := collectFiles(dir, files); err != nil {
			log.Fatal(err)
		}
		close(files)
	}()

	errorChan := make(chan error)
	resultsChan := make(chan ParseResult)
	var processErr error

WorkLoop:
	for {
		select {
		case f, ok := <-files:
			if !ok {
				fmt.Println("channel closed")
				files = nil
				close(errorChan)
				close(resultsChan)
				break WorkLoop
			}
			go func() {
				parseResult, err := processFile(f, term, bufSize)
				if err != nil {
					errorChan <- err
				}
				resultsChan <- parseResult
			}()
		case err, ok := <-errorChan:
			if !ok {
				errorChan = nil
				break WorkLoop
			}
			processErr = errors.Wrap(processErr, err.Error())
		case result, ok := <-resultsChan:
			if !ok {
				resultsChan = nil
				break WorkLoop
			}
			res = append(res, result)
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

func processFile(path string, term string, bufSize int) (ParseResult, error) {
	parseResult := ParseResult{path: path}
	var f *os.File
	f, err := os.Open(path)
	if err != nil {
		return parseResult, err
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
			parseResult.offsets = append(parseResult.offsets, int64(offset+pos))
		}
		offset += n
	}

	return parseResult, err
}
