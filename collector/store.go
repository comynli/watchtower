package main

import (
	"net/http"
	"github.com/lixm/watchtower/metrics"
	"log"
	"github.com/lixm/watchtower/context"
	"strings"
	"time"
	"math/rand"
	tomb "gopkg.in/tomb.v2"
)


type Storage struct {
	queue chan metrics.Metrics
	tomb.Tomb
}

func NewStorage(queue chan metrics.Metrics) *Storage {
	return &Storage{queue: queue}
}

func (s *Storage) store(m metrics.Metrics) {
	jStream, err := m.ToJsonStream()
	if err != nil {
		log.Printf("store %v fail: %s", m, err)
		return
	}

	esUrls := strings.Split(context.GetString("elasticsearch.url"), ",")
	rand.Seed(time.Now().UnixNano())
	idx := rand.Intn(len(esUrls))
	_, err := http.DefaultClient.Post(esUrls[idx], "application/json", jStream)
	if err != nil {
		log.Printf("store %v fail: %s", m, err)
	}
	//TODO process resp
}

func (s *Storage) Start() {
	for {
		select {
		case <- s.Dying():
			return
		case m := <- s.queue:
			s.store(m)
		}
	}
}

func (s *Storage) Stop() error {
	s.Kill(nil)
	return s.Wait()
}
