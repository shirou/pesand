package main

import (
	"testing"
)

func TestStoredQueue(t *testing.T) {
	q := NewStoredQueue(1)

	if q.Get() != "" {
		t.Errorf("wrong empty handling")
	}

	q.Put("test")
	if q.Get() != "test" {
		t.Errorf("not match")
	}
	q.Put("test1")
	q.Put("test2")
	if q.Get() != "test2" {
		t.Errorf("wrong max handling")
	}

}
