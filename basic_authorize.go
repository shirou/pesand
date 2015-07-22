package main

import ()

type BasicAuth struct {
	username string
	password string
}

func (b *BasicAuth) Auth(username string, password string) bool {
	if username != b.username {
		return false
	}
	if password != b.password {
		return false
	}
	return true
}

func NewBasicAuth() *BasicAuth {
	return &BasicAuth{}
}
