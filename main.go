package main

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"log"
	"math"
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
	parseErrors  chan error
	parseResults chan ParseResult
	term         string
	bufSize      int
	workerCount  int
	files        chan string
	curFile      int
}

func ParallelParse(dir string, term string, bufSize int, workerCount int) ([]ParseResult, error) {
	var res []ParseResult

	files := make(chan string)
	filesErr := make(chan error, 1)
	totalFilesChan := make(chan int, 1)
	go func() {
		n, err := sendFiles(dir, files)
		if err != nil {
			filesErr <- err
		} else {
			totalFilesChan <- n
		}
		close(files)
		close(filesErr)
		close(totalFilesChan)
	}()

	p := &FileProcessor{
		parseErrors:  make(chan error),
		parseResults: make(chan ParseResult),
		term:         term,
		bufSize:      bufSize,
		workerCount:  workerCount,
		files:        files,
	}
	go func() {
		p.Do()
	}()

	var errs []string
	var filesProcessed int
	totalFiles := math.MaxInt32
	for totalFiles > filesProcessed {
		select {
		case tf, ok := <-totalFilesChan:
			if !ok {
				totalFilesChan = nil
				continue
			}
			totalFiles = tf
		case err, ok := <-filesErr:
			if !ok {
				filesErr = nil
			}
			if err != nil {
				return res, err
			}
		case pr := <-p.parseResults:
			filesProcessed++
			res = append(res, pr)
		case err := <-p.parseErrors:
			filesProcessed++
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return res, errors.New(strings.Join(errs, "\n"))
	} else {
		return res, nil
	}
}

func (p *FileProcessor) Do() {
	workers := make(chan struct{}, p.workerCount)
	for i := 0; i < p.workerCount; i++ {
		workers <- struct{}{}
	}

	for {
		select {
		case <-workers:
			f := <-p.files
			if f == "" {
				continue
			}
			go func() {
				pr, err := processFile(f, p.term, p.bufSize)
				if err != nil {
					p.parseErrors <- err
				} else {
					p.parseResults <- pr
				}
				workers <- struct{}{}
			}()
		}
	}

	//close(p.parseErrors)
	//close(p.parseResults)
	//close(workers)
}

func (p *FileProcessor) DoNoWorker() {
	for {
		select {
		case f := <-p.files:
			if f == "" {
				continue
			}
			go func() {
				pr, err := processFile(f, p.term, p.bufSize)
				if err != nil {
					p.parseErrors <- err
				} else {
					p.parseResults <- pr
				}
			}()
		}
	}

	//close(p.parseErrors)
	//close(p.parseResults)
	//close(workers)
}

func sendFiles(dir string, out chan<- string) (int, error) {
	var n int
	err := filepath.WalkDir(dir, func(path string, dir os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if dir.IsDir() {
			return err
		}
		out <- path
		n++
		return nil
	})
	return n, err
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
