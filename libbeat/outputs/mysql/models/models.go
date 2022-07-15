package models

import (
	"errors"
	"sync"
)

var (
	ModelNotFoundErr = errors.New("model not found")
)
var models = map[string]sync.Pool{
	"log": sync.Pool{New: func() any { return new(Log) }},
}

func Get(name string) (any, error) {
	pool, ok := models[name]
	if !ok {
		return nil, ModelNotFoundErr
	}
	return pool.Get(), nil
}

func Put(name string, model any) error {
	pool, ok := models[name]
	if !ok {
		return ModelNotFoundErr
	}
	pool.Put(model)
	return nil
}
