package dendroaspis

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/mrb/riakpbc"
)

type Image struct {
	Bytes []byte
	Text  string `riak:"index" json:"text"`
	Date  int64  `riak:"index" json:"date"`
}

type Storage struct {
	client *riakpbc.Client
	bucket string
}

func NewStorage(nodeList []string, bucketName string) Storage {
	coder := riakpbc.NewCoder("json", riakpbc.JsonMarshaller, riakpbc.JsonUnmarshaller)
	riakCoder := riakpbc.NewClientWithCoder(nodeList, coder)
	return Storage{
		client: riakCoder,
		bucket: bucketName,
	}
}

func (s *Storage) Dial() error {
	return s.client.Dial()
}

func (s *Storage) Close() error {
	return s.Close()
}

func (s *Storage) GetById(id string) (Image, error) {
	out := &Image{}
	_, err := s.client.FetchStruct(s.bucket, id, out)
	return *out, err
}

func (s *Storage) Store(img Image) (string, error) {
	if key, err := buildKey(img); err != nil {
		return "", err
	} else {
		_, err := s.client.StoreStruct(s.bucket, key, &img)
		return key, err
	}
}

func buildKey(img Image) (string, error) {
	sha := sha256.New()
	_, err := sha.Write(img.Bytes)
	if err != nil {
		return "", err
	} else {
		return hex.EncodeToString(sha.Sum(nil)), nil
	}
}
