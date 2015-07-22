package main

import ()

type Authorize interface {
	Auth(username string, password string) bool
}
