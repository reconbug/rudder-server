package model

import "sync"

const GZipBestSpeed = 1

type File struct {
	Path string
	Mu   *sync.RWMutex
}
