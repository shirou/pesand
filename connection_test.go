package main

import (
	//"fmt"
	"testing"
)

func TestStoredQueue(t *testing.T) {
	q := NewStoredQueue()

	result, _ := q.Pop()
	if result != "" {
		t.Errorf("wrong empty handling")
	}

	q.Push("test")
	result, _ = q.Pop()
	if result != "test" {
		t.Errorf("not match")
	}
	//q.Push("test1")
	//q.Push("test2")
	//result, _ = q.Pop()
	//if result != "test2" {
	//	t.Errorf("wrong max handling")
	//}

}
