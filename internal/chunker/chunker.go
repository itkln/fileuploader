package chunker

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/itkln/fileuploader/internal/models"
	"io"
	"os"
	"sync"
)

const (
	ErrOpenFileFailed   = "open file failed"
	ErrReadFileFailed   = "read file: failed"
	ErrCreateFileFailed = "create chunk file: failed"
)

type DefaultFileChunker struct {
	chunkSize int
}

type DefaultUploader struct {
	serverURL string
}

func (c *DefaultFileChunker) Chunk(filepath string) ([]models.ChunkMeta, error) {
	var chunks []models.ChunkMeta

	file, err := os.Open(filepath)
	if err != nil {
		return nil, errors.New(ErrOpenFileFailed)
	}
	defer file.Close()

	buf := make([]byte, c.chunkSize)
	index := 0

	for {
		bytesRead, err := file.Read(buf)
		if err != nil && err != io.EOF {
			return nil, errors.New(ErrReadFileFailed)
		}
		if bytesRead == 0 {
			break
		}

		hash := md5.Sum(buf[:bytesRead])
		hashString := hex.EncodeToString(hash[:])

		chunkFileName := fmt.Sprintf("%s.chunk.%d", filepath, index)
		chunkFile, err := os.Create(chunkFileName)
		if err != nil {
			return nil, errors.New(ErrCreateFileFailed)
		}

		if _, err = chunkFile.Write(buf[:bytesRead]); err != nil {
			chunkFile.Close()
			return nil, err
		}
		chunkFile.Close()

		chunks = append(chunks, models.ChunkMeta{
			FileName: chunkFileName,
			MD5Hash:  hashString,
			Index:    index,
		})
		index++
	}

	return chunks, nil
}

func (c *DefaultFileChunker) ChunkLargeFile(filepath string) ([]models.ChunkMeta, error) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var chunks []models.ChunkMeta

	file, err := os.Open(filepath)
	if err != nil {
		return nil, errors.New(ErrOpenFileFailed)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, errors.New("get file info failed")
	}

	numChunks := int(fileInfo.Size() / int64(c.chunkSize))
	if fileInfo.Size()%int64(c.chunkSize) != 0 {
		numChunks++
	}

	chunkChan := make(chan models.ChunkMeta, numChunks)
	errChan := make(chan error, 1)
	indexChan := make(chan int, numChunks)

	for i := 0; i < numChunks; i++ {
		indexChan <- i
	}
	close(indexChan)

	for i := 0; i < 4; i++ { // Number of parallel workers
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range indexChan {
				offset := int64(index) * int64(c.chunkSize)
				buf := make([]byte, c.chunkSize)

				_, err := file.Seek(offset, io.SeekStart)
				if err != nil {
					errChan <- err
					return
				}

				bytesRead, err := file.Read(buf)
				if err != nil && err != io.EOF {
					errChan <- err
					return
				}

				if bytesRead > 0 {
					hash := md5.Sum(buf[:bytesRead])
					hashString := hex.EncodeToString(hash[:])

					chunkFileName := fmt.Sprintf("%s.chunk.%d", filepath, index)
					chunkFile, err := os.Create(chunkFileName)
					if err != nil {
						errChan <- err
						return
					}

					if _, err = chunkFile.Write(buf[:bytesRead]); err != nil {
						chunkFile.Close()
						errChan <- err
						return
					}
					chunkFile.Close()

					chunk := models.ChunkMeta{
						FileName: chunkFileName,
						MD5Hash:  hashString,
						Index:    index,
					}

					mu.Lock()
					chunks = append(chunks, chunk)
					mu.Unlock()

					chunkChan <- chunk
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(chunkChan)
		close(errChan)
	}()

	for err := range errChan {
		if err != nil {
			return nil, err
		}
	}

	return chunks, nil
}
