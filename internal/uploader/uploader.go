package uploader

import (
	"bytes"
	"errors"
	"github.com/itkln/fileuploader/internal/models"
	"net/http"
	"os"
)

type DefaultUploader struct {
	serverURL string
	client    *http.Client
}

func NewDefaultUploader(serverURL string) *DefaultUploader {
	return &DefaultUploader{
		serverURL: serverURL,
		client:    &http.Client{},
	}
}

func (c *DefaultUploader) UploadChunk(chunk models.ChunkMeta) error {
	data, err := os.ReadFile(chunk.FileName)
	if err != nil {
		return errors.New("failed to read chunk file: " + err.Error())
	}

	req, err := http.NewRequest(http.MethodPost, c.serverURL, bytes.NewReader(data))
	if err != nil {
		return errors.New("failed to create HTTP request: " + err.Error())
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return errors.New("failed to send HTTP request: " + err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.New("upload failed with status code: " + resp.Status)
	}

	return nil
}
