package main

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/dgrijalva/jwt-go"
)

type JWTAuth struct {
	jwtKey string
}

func (b *JWTAuth) Auth(username string, jwtoken string) bool {

	token, err := jwt.Parse(jwtoken, func(token *jwt.Token) (interface{}, error) {

		//validate alg
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return b.jwtKey, nil
	})
	if err != nil {
		log.Error(err)
		return false
	}
	if !token.Valid {
		return false
	}

	userid := token.Claims["sub"].(string) //get userid from token
	if username != userid {
		return false
	}

	//everything passes
	return true

}

func NewJWTAuth(key string) *JWTAuth {
	return &JWTAuth{
		jwtKey: key,
	}
}
