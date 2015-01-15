package main

import (
	"io"
	"os/exec"
	"github.com/lixm/watchtower/metrics"
	"os"
	"time"
	"errors"
	"fmt"
	"syscall"
	tomb "gopkg.in/tomb.v2"
	"strings"
	"log"
	"github.com/lixm/watchtower/context"
)


type Process struct {
	maxTime int
	exe string
	cmd *exec.Cmd
	cost int64
}


func NewProcess(cmd string, maxTime int, env []string) *Process {
	process := &Process{maxTime: maxTime, exe: cmd}
	process.cmd = exec.Command(cmd)
	process.cmd.Env = append(env, os.Environ())
	return process
}


func (p *Process) Run() ([]metrics.Metrics, error) {
	start := time.Now().UnixNano()
	var ms []metrics.Metrics
	err := make(chan error)
	proc := make(chan *os.Process)
	out := make(chan *io.ReadCloser)
	go func(proc chan *os.Process, out chan *io.ReadCloser, err error) {
		r, e :=p.cmd.StdoutPipe()
		if e != nil {
			err <- e
		}
		e = p.cmd.Start()
		if e != nil {
			err <- e
		}
		proc <- p.cmd.Process
		e = p.cmd.Wait()
		if e != nil {
			err <- e
		}
		out <- &r
	}(proc, out, err)
	process := <- proc
	for{
		select {
		case output := <- out:
			mCh := make(chan metrics.Metrics)
			metrics.FromLineStream(output, mCh, err)
			for m := range <- mCh {
				ms = append(ms, m)
			}
			p.cost = time.Now().UnixNano() - start
			return ms, nil
		case e := <- err:
			p.cost = time.Now().UnixNano() - start
			return ms, e
		case <- time.After(time.Duration(p.maxTime) * time.Second):
			process.Signal(syscall.SIGKILL)
			p.cost = time.Now().UnixNano() - start
			return ms, errors.New(fmt.Sprintf("execute %s timeout", p.exe))
		}
	}
}


func (p *Process) Kill() {
	p.cmd.ProcessState
}

func (p *Process) IsAlive() bool{
	if p.cmd.Process == nil {
		return false
	}
	if p.cmd.ProcessState != nil && p.cmd.ProcessState.Exited() {
		return false
	}
	return true
}

func (p *Process) Times() (int64, int64, int64){
	if p.cmd.ProcessState != nil && p.cmd.ProcessState.Exited() {
		return p.cmd.ProcessState.UserTime().Nanoseconds(), p.cmd.ProcessState.SystemTime().Nanoseconds(), p.cost
	}
	return 0, 0, p.cost
}

type Executor struct {
	Name string
	Group string
	env []string
	cmd string
	maxTime int
	Interval int
	Output chan metrics.Metrics
	IsStarted bool
	tomb.Tomb
}


func NewExecutor(name, group, cmd string, maxTime, interval int, env []string, out chan metrics.Metrics) *Executor {
	if maxTime > interval {
		maxTime = interval
	}
	executor := &Executor{Name: name, Group: group, cmd: cmd, env: env, maxTime: maxTime, Interval: interval, Output: out}
	return executor

}

func (e *Executor) getExec() string {
	return context.GetString("dir.plugin") + e.cmd
}

func (e *Executor) Start() {
	p := NewProcess(e.getExec(), e.maxTime, e.env)
	e.IsStarted = true
	defer func() {e.IsStarted = false} ()
	for {
		select {
		case <- time.After(time.Duration(e.Interval) * time.Second):
			if !p.IsAlive() {
				if ms, err := p.Run(); err == nil {
					for _, m := range ms {
						e.Output <- m
					}
				}
				if context.GetBool(context.GetString("cluster.name") + "::" + "plugin.stats." + e.Name){
					sy, us, to := p.Times()
					e.Output <- metrics.New(strings.Join(([]string{e.Group, e.Name, "_sys"}), "."),
						"_system", nil, nil, float64(sy))
					e.Output <- metrics.New(strings.Join(([]string{e.Group, e.Name, "_user"}), "."),
						"_system", nil, nil, float64(us))
					e.Output <- metrics.New(strings.Join(([]string{e.Group, e.Name, "_total"}), "."),
						"_system", nil, nil, float64(to))
				}

			}else {
				log.Printf("%s is still running", e.Name)
			}
		case <- e.Dying():
			if p.IsAlive() {
				p.Kill()
			}
			return
		}
	}
}


func (e *Executor) Stop() error {
	e.IsStarted = false
	e.Kill(nil)
	return e.Wait()
}


type ExecutorManager struct {
	executors map[string]*Executor
	Output chan metrics.Metrics
	tomb.Tomb
}

func NewExecutorManager(queue chan metrics.Metrics) *ExecutorManager {
	em := &ExecutorManager{}
	em.executors = make(map[string] *Executor)
	em.Output = queue
	return em
}

func (em *ExecutorManager) checkExecutor(name string, running bool) {
	if _, ok := em.executors[name]; !ok {
		group := context.GetString(context.GetString("cluster.name") + "::" + "plugin.group." + name)
		cmd := context.GetString(context.GetString("cluster.name") + "::" + "plugin.cmd." + name)
		maxTime := context.GetInt(context.GetString("cluster.name") + "::" + "plugin.maxTime." + name)
		interval := context.GetInt(context.GetString("cluster.name") + "::" + "plugin.interval." + name)
		env := strings.Split(context.GetString(context.GetString("cluster.name") + "::" + "plugin.env." + name), ",")
		executor := NewExecutor(name, group, cmd, maxTime, interval, env, em.Output)
		em.executors[name] = executor
	}

	if(running) {
		if !em.executors[name].IsStarted {
			go em.executors[name].Start()
		}
	}else{
		if em.executors[name].IsStarted {
			em.executors[name].Stop()
		}
	}
}

func (em *ExecutorManager) checkAll() {
	for _, key := range context.SuffixKeys(context.GetString("cluster.name") + "::" + "plugin.name") {
		name := context.GetString(key)
		running := context.GetBool(context.GetString("cluster.name") + "::" + "plugin.running." + name)
		em.checkExecutor(name, running)
	}
}

func (em *ExecutorManager) Start() {
	for{
		select {
		case <- time.After(time.Duration(context.GetInt("time.check")) * time.Second):
			em.checkAll()
		case <- em.Dying():
			return
		}
	}
}


func (em *ExecutorManager) Stop() error {
	em.Kill(nil)
	return em.Wait()
}
