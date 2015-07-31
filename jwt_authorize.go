package main

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/dgrijalva/jwt-go"
)

type JWTAuth struct {
	allowAnonymous bool
	jwtKey         []byte
}

func (j *JWTAuth) Auth(username string, jwtoken string) bool {

	token, err := jwt.Parse(jwtoken, func(token *jwt.Token) (interface{}, error) {

		//validate alg
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return j.jwtKey, nil
	})
	if err != nil {
		log.Error(err)
		return false
	}
	if !token.Valid {
		log.WithField("token", jwtoken).Warn("Token not valid")
		return false
	}

	userid := token.Claims["sub"].(string) //get userid from token
	if username != userid {

		log.WithFields(log.Fields{
			"conn user": username,
			"jwt user":  userid,
		}).Debug("jwtAuth Username")

		return false
	}

	//everything passes
	return true

}

func (j *JWTAuth) AllowAnon() bool {
	return j.allowAnonymous
}

func NewJWTAuth(key string, anon bool) *JWTAuth {
	return &JWTAuth{
		allowAnonymous: anon,
		jwtKey:         []byte(key),
	}
}
