package models

type ChunkMeta struct {
	FileName string `json:"file_name"`
	MD5Hash  string `json:"md5_hash"`
	Index    int    `json:"index"`
}
