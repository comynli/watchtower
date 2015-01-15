package metrics

import (
	"io"
	"fmt"
	"encoding/json"
	"encoding/gob"
	"strings"
	"errors"
	"strconv"
	"bufio"
	"bytes"
	"time"
	"net"
)

type Metrics struct {
	Name string `json:"name"`
	Group string `json:"group"`
	Time int64 `json:"time"`
	Value float64 `json:"value"`
	Source string `json:"source"`
}

func New(name, group, source string, time int64, value float64) Metrics {
	m := Metrics{Name: name, Group: group, Value: value, Time: time, Source: source}
	if m.Time == nil {
		m.Time = time.Now().UnixNano()
	}
	return m
}


func FromJsonString(jStr string) (Metrics, error) {
	jsonBlob := []byte(jStr)
	var metrics Metrics
	err := json.Unmarshal(jsonBlob, &metrics)
	if err != nil {
		return nil, err
	}
	return metrics, nil
}


func FromJsonStream(stream net.Conn, out chan Metrics, err chan error) {
	dec := json.NewDecoder(stream)
	for {
		var metrics Metrics
		if e := dec.Decode(&metrics); e == io.EOF {
			break
		} else if e != nil {
			err <- e
		}
		out <- SetupSource(metrics, stream.RemoteAddr().String())
	}
}


func FromGobStream(stream net.Conn, out chan Metrics, err chan error) {
	dec := gob.NewDecoder(stream)
	for {
		var metrics Metrics
		if e := dec.Decode(&metrics); e == io.EOF {
			break
		} else if e != nil {
			err <- e
		}
		out <- SetupSource(metrics, stream.RemoteAddr().String())
	}
}

func FromLineString(line string) (Metrics, error) {
	var err error
	arr := strings.Split(line, ";")
	if len(arr) < 4 {
		return nil, errors.New("parse error, now enough fileds")
	}
	metrics := &Metrics{}
	metrics.Name = strings.TrimSpace(arr[0])
	metrics.Group = strings.TrimSpace(arr[1])
	metrics.Time, err = strconv.ParseInt(arr[2], 0, 64)
	if err != nil {
		metrics.Time = time.Now().UnixNano()
	}
	metrics.Value, err = strconv.ParseFloat(arr[3], 64)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("parse value errror, %s", err))
	}
	if len(arr) > 4 {
		metrics.Source = arr[4]
	}
	return metrics, nil
}

func FromLineStream(stream net.Conn, out chan Metrics, err chan error) {
	reader := bufio.NewReader(stream)
	for {
		line, e := reader.ReadString((byte)('\n')); if e == io.EOF {
			break
		}else if e != nil {
			err <- e
		}
		m, e := FromLineString(line)
		if e == nil {
			out <- SetupSource(m, stream.RemoteAddr().String())
		}else {
			err <- e
		}
	}
}

func (m Metrics) ToJson() ([]byte, error) {
	buf, err := json.Marshal(m)
	return buf, err
}

func (m Metrics) ToJsonString() (string, error) {
	buf, err := json.MarshalIndent(m, "", "    ")
	if err == nil {
		return string(buf), err
	}
	return nil, err
}

func (m Metrics) ToJsonStream() (io.Reader, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(buf)
	err := enc.Encode(m)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (m Metrics) ToGob() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(buf)
	err := enc.Encode(m)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}


func SetupSource(m Metrics, source string) Metrics {
	if m.Source == nil {
		m.Source = source
	}
	return m
}
