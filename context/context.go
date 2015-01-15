package context

import (
	"sync"
	"strings"
	"errors"
)

var (
	mutex sync.Mutex
	data = make(map[string]interface {})
)

func Set(key string, val interface {}) {
	mutex.Lock()
	data[key] = val
	mutex.Unlock()
}


func BatchSet(keys []string, vals []interface {}) error {
	if len(keys) != len(vals) {
		return errors.New("values not match keys")
	}
	mutex.Lock()
	for idx, key := range keys {
		data[key] = vals[idx]
	}
	mutex.Unlock()
	return nil
}

func Get(key string) interface {} {
	mutex.Lock()
	val := data[key]
	mutex.Unlock()
	return val
}

func Exist(key string) bool {
	mutex.Lock()
	_, ok := data[key]
	mutex.Unlock()
	return ok
}

func Delete(key string) {
	mutex.Lock()
	delete(data, key)
	mutex.Unlock()
}

func Clean() {
	mutex.Lock()
	data = make(map[string]interface {})
	mutex.Unlock()
}

func Keys()[]string {
	mutex.Lock()
	var keys []string
	for key, _ := range data {
		keys = append(keys, key)
	}
	mutex.Unlock()
	return keys
}

func SuffixKeys(suffix string) []string {
	mutex.Lock()
	var keys []string
	for key, _ := range data {
		if strings.HasSuffix(key, suffix) {
			keys = append(keys, key)
		}
	}
	mutex.Unlock()
	return keys
}

func GetBool(key string) bool {
	val := Get(key)
	return val.(bool)
}


func GetString(key string) string {
	return Get(key).(string)
}

func GetInt(key string) int {
	return Get(key).(int)
}
