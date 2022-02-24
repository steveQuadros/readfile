package main

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
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

type FileProcessor struct {
	parseErrors  chan<- error
	parseResults chan<- ParseResult
	term         string
	bufSize      int
	workerCount  int
	files        []string
	curFile      int
}

// Parallel version walks a dir to get files to parse and pushes them to chan skipping dirs
// workers pull from the filenames and do the parsing
func ParallelParse(dir string, term string, bufSize int, workerCount int) ([]ParseResult, error) {
	var res []ParseResult

	files := make(chan []string, 1)
	filesErr := make(chan error, 1)

	go func() {
		filepaths, err := collectFiles(dir)
		if err != nil {
			filesErr <- err
		} else {
			files <- filepaths
		}
	}()

	parseErrors := make(chan error)
	parseResults := make(chan ParseResult)

	var fileCount int
	select {
	case filesToProcess := <-files:
		p := &FileProcessor{
			parseErrors:  parseErrors,
			parseResults: parseResults,
			term:         term,
			bufSize:      bufSize,
			workerCount:  workerCount,
			files:        filesToProcess,
		}
		p.Do()
	case err := <-filesErr:
		return res, err
	}

	var errs []string
	for fileCount > 0 {
		select {
		case pr := <-parseResults:
			fileCount--
			res = append(res, pr)
		case err := <-parseErrors:
			fileCount--
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return res, errors.New(strings.Join(errs, "; "))
	} else {
		return res, nil
	}
}

func (p *FileProcessor) Do() {
	workers := make(chan struct{}, p.workerCount)
	for i := 0; i < p.workerCount; i++ {
		workers <- struct{}{}
	}

	for p.curFile < len(p.files) {
		select {
		case <-workers:
			f := p.files[p.curFile]
			p.curFile++
			go func() {
				res, err := processFile(f, p.term, p.bufSize)
				if err != nil {
					p.parseErrors <- err
				} else {
					p.parseResults <- res
				}
			}()
			workers <- struct{}{}
		}
	}
}

func collectFiles(dir string) (files []string, err error) {
	err = filepath.WalkDir(dir, func(path string, dir os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if dir.IsDir() {
			return err
		}
		files = append(files, path)
		return nil
	})
	return files, err
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
