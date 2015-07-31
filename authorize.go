package main

import (
	log "github.com/Sirupsen/logrus"
)

type Authorize interface {
	Auth(username string, password string) bool
	AllowAnon() bool
}

func (c *Config) ConfigAuth() Authorize {

	if c.Auth.JWTKey != "" {
		log.WithField("type", "jwtAuth").Info("Authorization")
		return Authorize(NewJWTAuth(c.Auth.JWTKey, c.Auth.AllowAnonymous))
	}
	if c.Auth.Username != "" && c.Auth.Password != "" {
		log.WithField("type", "basicAuth").Info("Authorization")
		return Authorize(NewBasicAuth(c.Auth.Username, c.Auth.Password, c.Auth.AllowAnonymous))
	}

	log.WithField("type", "none").Warn("Config Authorization")
	return nil
}
