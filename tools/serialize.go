package tools

import (
	"bytes"
	"encoding/gob"
)

func Serialize(data interface{}) []byte {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(data)
	if err != nil {
		return nil
	}
	return buf.Bytes()
}

func Deserialize(data []byte, result interface{}) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(result)
	return err
}
