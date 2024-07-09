package internal

import (
	"github.com/itkln/fileuploader/internal/models"
	"sync"
)

type Uploader interface {
	UploadChunk(chunk models.ChunkMeta) error
}

func synchronizeChunks(chunks []models.ChunkMeta, metadata map[string]models.ChunkMeta, uploader Uploader, wg *sync.WaitGroup, mu *sync.Mutex) error {
	chunkChan := make(chan models.ChunkMeta, len(chunks)) // Channel to send chunks to workers
	errChan := make(chan error, len(chunks))              // Channel to receive errors from workers

	// Add chunks to the channel
	for _, chunk := range chunks {
		chunkChan <- chunk
	}

	close(chunkChan)

	// Start multiple goroutines to process chunks in parallel
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range chunkChan {
				if err := processChunk(chunk, metadata, uploader, mu); err != nil {
					errChan <- err
				}
			}
		}()
	}

	// Wait for all goroutines to finish
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Check for errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}
	return nil
}

func processChunk(chunk models.ChunkMeta, metadata map[string]models.ChunkMeta, uploader Uploader, mu *sync.Mutex) error {
	newHash := chunk.MD5Hash

	mu.Lock()
	oldChunk, exists := metadata[chunk.FileName]
	mu.Unlock()

	if !exists || oldChunk.MD5Hash != newHash {
		if err := uploader.UploadChunk(chunk); err != nil {
			return err
		}

		mu.Lock()
		metadata[chunk.FileName] = chunk
		mu.Unlock()
	}
	return nil
}
