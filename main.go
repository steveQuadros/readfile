package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"go.uber.org/zap"
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

type FileLocation struct {
	pos int
}

// @TODO - for each file create offset and count based on buffer using index from strings.Index()
func SerialParse(dir string, term string, bufSize int) (map[string][]int64, error) {
	out := make(map[string][]int64)
	err := filepath.WalkDir(dir, func(path string, dir os.DirEntry, err error) error {
		if dir.IsDir() {
			return nil
		}
		b := make([]byte, bufSize)
		var f *os.File
		f, err = os.Open(path)
		if err != nil {
			return err
		}
		defer func() {
			err = f.Close()
		}()

		termBytes := []byte(term)
		for {
			_, err = f.Read(b)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			if bytes.Contains(b, termBytes) {
				var inf os.FileInfo
				inf, err = dir.Info()
				if err != nil {
					return err
				}
				out[path] = append(out[path], inf.Size())
			}
		}

		return nil
	})
	return out, err
}

func readBinary() {
	s := "Hello World!"
	b := []byte{}
	for i := 0; i < 100; i++ {
		b = append(b, []byte(s)...)
	}
	buf := bytes.NewReader(b)
	out := make([]byte, 10)
	var err error
	for {
		err = binary.Read(buf, binary.LittleEndian, &out)
		if err != nil {
			fmt.Println("binary.Read failed:", err)
		}
		if err == io.EOF {
			return
		}
		fmt.Print(string(out))
	}
}

func writeBin() {
	BufSize := 100
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatal(err)
	}

	file, err := os.Open("example.log")
	if err != nil {
		logger.Fatal(err.Error())
	}
	defer func() {
		if err = file.Close(); err != nil {
			logger.Fatal(err.Error())
		}
	}()

	b := make([]byte, BufSize)
	out, err := os.OpenFile("examplebin.log", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer func() {
		if err = out.Close(); err != nil {
			logger.Fatal(err.Error())
		}
	}()

	for {
		var n int
		n, err = file.Read(b)
		if err != nil {
			if err == io.EOF {
				return
			}
			logger.Fatal(err.Error())
		}
		fmt.Println(string(b[0:n]))
		err = binary.Write(out, binary.LittleEndian, b)
		if err != nil {
			logger.Fatal(err.Error())
		}
	}
}
