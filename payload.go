package main

import (
	"fmt"
	"io"
)

// An intPayload implements proto.Payload, and is an int64 that
// formats itself and then prints itself into the payload.
type intPayload string

func newIntPayload(i int64) intPayload {
	return intPayload(fmt.Sprint(i))
}
func (ip intPayload) ReadPayload(r io.Reader) error {
	return nil
}
func (ip intPayload) ToString() string {
	return string(ip)
}
func (ip intPayload) WritePayload(w io.Writer) error {
	_, err := w.Write([]byte(string(ip)))
	return err
}
func (ip intPayload) Size() int {
	return len(ip)
}

// String payload
type stringPayload string

func newStringPayload(s string) stringPayload {
	return stringPayload(s)
}
func (sp stringPayload) ToString() string {
	return string(sp)
}
func (sp stringPayload) ReadPayload(r io.Reader) error {
	// not implemented
	return nil
}
func (sp stringPayload) WritePayload(w io.Writer) error {
	_, err := w.Write([]byte(sp))
	return err
}
func (sp stringPayload) Size() int {
	return len(sp)
}
