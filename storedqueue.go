package main

import (
	"sync"
)

type StoredQueue struct {
	List []string
	m    sync.Mutex
}

//NewStoredQueue
func NewStoredQueue() *StoredQueue {
	return &StoredQueue{List: make([]string, 0, 10)}
}

//Push
func (q *StoredQueue) Push(elem string) {
	q.m.Lock()
	defer q.m.Unlock()
	//add the elm to the slice
	q.List = append(q.List, elem)
}

//Pop
func (q *StoredQueue) Pop() (string, bool) {
	q.m.Lock()
	defer q.m.Unlock()
	//check the length
	if len(q.List) == 0 {
		return "", false
	}

	elm := q.List[0]    //get the first elm
	q.List = q.List[1:] //remove the first elm

	return elm, true
}

//Size
func (q *StoredQueue) Size() int {
	q.m.Lock()
	defer q.m.Unlock()
	return len(q.List)
}

//IsEmpty
func (q *StoredQueue) IsEmpty() bool {
	q.m.Lock()
	defer q.m.Unlock()
	return len(q.List) == 0
}
