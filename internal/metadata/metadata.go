package metadata

import (
	"encoding/json"
	"errors"
	"github.com/itkln/fileuploader/internal/models"
	"os"
)

type DefaultMetadata struct{}

func (m *DefaultMetadata) LoadMetadata(filepath string) (map[string]models.ChunkMeta, error) {
	metadata := make(map[string]models.ChunkMeta)

	data, err := os.ReadFile(filepath)
	if err != nil {
		return metadata, errors.New("could not read metadata file")
	}

	err = json.Unmarshal(data, &metadata)
	if err != nil {
		return metadata, errors.New("could not unmarshal metadata file")
	}

	return metadata, nil
}

func (m *DefaultMetadata) SaveMetadata(filepath string, metadata map[string]models.ChunkMeta) error {
	data, err := json.Marshal(metadata)
	if err != nil {
		return errors.New("could not marshal metadata file")
	}

	err = os.WriteFile(filepath, data, 0644)
	if err != nil {
		return err
	}

	return nil
}
