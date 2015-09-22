package main

import (
	log "github.com/Sirupsen/logrus"
)

//similar to http basic auth, checks against 1 username:password set in conf
type BasicAuth struct {
	allowAnonymous bool
	username       string
	password       string
}

func (b *BasicAuth) Auth(username string, password string) bool {

	log.WithFields(log.Fields{
		"conn user":     username,
		"required user": b.username,
	}).Debug("basicAuth Username")

	if username != b.username {
		return false
	}

	log.WithFields(log.Fields{
		"conn pass":     password,
		"required pass": b.password,
	}).Debug("basicAuth Password")

	if password != b.password {
		return false
	}
	return true
}

func (b *BasicAuth) AllowAnon() bool {
	return b.allowAnonymous
}

func NewBasicAuth(user string, pass string, anon bool) *BasicAuth {
	return &BasicAuth{
		allowAnonymous: anon,
		username:       user,
		password:       pass,
	}
}
