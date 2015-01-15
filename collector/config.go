package main

import (
	yaml "gopkg.in/yaml.v2"
	"io"
	"log"
	"io/ioutil"
	"errors"
	"github.com/lixm/watchtower/context"
	"strings"
	"encoding/gob"
	tomb "gopkg.in/tomb.v2"
	"path/filepath"
	"os"
)


type localConfig struct {
	clusterName string `cluster.name`
	clusterMode string `cluster.mode`
	mailBoxSize int `mailbox.size`
	upStreams string  `upstream.servers`
	configServer string `config.server`
	listenerText int `listener.text`
	listenerJson int `listener.json`
	listenerGob  int `listener.gob`
	listenerConfig  int `listener.config`
	esUrls string `elasticsearch.url`
	pluginDir string `dir.plugin`
}


func readLocalConfig(path string) error{
	var conf localConfig
	content, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatalf("read local config fail %v\n", err)
		return err
	}
	err = yaml.Unmarshal(content, &conf)
	if err != nil {
		return err
	}
	if conf.clusterName == nil || conf.clusterName == "" {
		log.Fatalf("cluster.name require")
		return errors.New("cluster.name require")
	}
	context.Set("cluster.name", conf.clusterName)
	context.Set("cluster.mode", "normal")
	if conf.clusterMode != nil && strings.ToLower(conf.clusterMode) != "store" {
		context.Set("cluster.mode", "store")
	}
	if conf.mailBoxSize == nil || conf.mailBoxSize < 1 {
		context.Set("mailbox.size", 1)
	}else {
		context.Set("mailbox.size", conf.mailBoxSize)
	}
	if conf.upStreams == nil || conf.upStreams == "" && context.GetString("cluster.mode") == "normal" {
		log.Fatalf("normal mode need upstream.servers")
		return errors.New("normal mode need upstream.servers")
	}
	if conf.listenerText == nil || conf.listenerText < 1 {
		context.Set("listener.text", 3900)
	}else {
		context.Set("listener.text", conf.listenerText)
	}
	if conf.listenerJson == nil || conf.listenerJson < 1 {
		context.Set("listener.json", 3800)
	}else {
		context.Set("listener.json", conf.listenerJson)
	}
	if conf.listenerGob == nil || conf.listenerGob < 1 {
		context.Set("listener.gob", 3700)
	}else {
		context.Set("listener.gob", conf.listenergob)
	}
	if context.GetString("cluster.mode") == "store" && (conf.esUrls == nil || conf.esUrls == "") {
		log.Fatalf("store mode nees elasticsearch.url")
		return errors.New("store mode nees elasticsearch.url")
	}

	if conf.pluginDir == nil || conf.pluginDir == "" {
		context.Set("dir.plugin", conf.pluginDir)
	}else {
		context.Set("dir.plugin", "plugin")
	}
	return nil
}


type PluginConfig struct {
	clusterName string
	name string
	group string
	cmd string
	maxTime int
	interval int
	env []string
	running bool
}


func ParsePluginConfig(stream io.Reader) error {
	dec := gob.NewDecoder(stream)
	for {
		var pc PluginConfig
		if e := dec.Decode(&pc); e == io.EOF {
			break
		} else if e != nil {
			return e
		}
		if pc.interval <= 0 {
			pc.interval = 60
		}
		if pc.maxTime > pc.interval {
				pc.maxTime == pc.interval
		}
		if pc.maxTime <= 0 {
				pc.maxTime == pc.interval
		}
		keys := []string{
			pc.clusterName + "::plugin.name" + pc.name,
			pc.clusterName + "::plugin.group" + pc.name,
			pc.clusterName + "::plugin.cmd" + pc.name,
			pc.clusterName + "::plugin.maxTime" + pc.name,
			pc.clusterName + "::plugin.interval" + pc.name,
			pc.clusterName + "::plugin.env" + pc.name,
			pc.clusterName + "::plugin.running" + pc.name,
		}
		values := []interface{} {
			pc.name,
			pc.group,
			pc.cmd,
			pc.maxTime,
			pc.interval,
			strings.Join(pc.env, ","),
			pc.running,
		}
		context.BatchSet(keys, values)
	}
}


func DumpPluginConfig(clusterName string, buf io.WriteCloser) error{
	enc := gob.NewEncoder(buf)
	for _, key := range context.SuffixKeys(context.GetString("cluster.name") + "::" + "plugin.name") {
		pc := PluginConfig{}
		name := context.GetString(key)
		pc.clusterName = clusterName
		pc.name = name
		pc.group = context.GetString(clusterName + "::plugin.group." + name)
		pc.cmd = context.GetString(clusterName + "::plugin.cmd." + name)
		pc.maxTime = context.GetInt(clusterName + "::plugin.maxTime." + name)
		pc.interval = context.GetInt(clusterName + "::plugin.interval." + name)
		pc.env = strings.Split(context.GetString(clusterName + "::plugin.env." + name), ",")
		pc.running = context.GetBool(clusterName + "::plugin.running." + name)
		enc.Encode(pc)
	}
	return buf.Close()
}


type ConfigService struct {
	tomb.Tomb
}

func NewConfigService() (*ConfigService, error) {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if(err != nil) {
		log.Fatalln(err)
		return nil, err
	}
	err =readLocalConfig(filepath.Join(dir, "config.yml"))
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}
	return &ConfigService{}, nil
}

func getConfig(clusterName string) {

}

func (cs *ConfigService) Start() {

}
