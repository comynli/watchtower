package main

import (
	"net"
	"github.com/lixm/watchtower/context"
	"log"
	"github.com/lixm/watchtower/metrics"
	tomb "gopkg.in/tomb.v2"
	"strings"
	"time"
)


type UpStream struct {
	MailBox chan metrics.Metrics
	tomb.Tomb
}

func NewUpStream() *UpStream {
	us := &UpStream{}
	us.MailBox = make(chan metrics.Metrics, context.GetInt("mailbox.size"))
	return us
}

func (us *UpStream) Receive(tp string) error {
	listener, err := net.Listen("tcp", context.GetString("listener." + tp))
	if err != nil {
		return err
	}
	defer listener.Close()

	for {
		select {
		case <- us.Dying():
			return nil
		default:

		}

		conn, err := listener.Accept()
		if err != nil {
			log.Printf("accept error: %s", err)
		}
		go us.receive(tp, conn)
	}
}

func (us *UpStream) receive(tp string, conn net.Conn) {
	err := make(chan error)
	switch tp{
	case "json":
		metrics.FromJsonStream(conn, us.MailBox, err)
	case "gob":
		metrics.FromGobStream(conn, us.MailBox, err)
	case "text":
		metrics.FromLineStream(conn, us.MailBox, err)
	}
}


func (us *UpStream) Send() {
	servers := strings.Split(context.GetString("upstream.servers"), ",")
	for _, server := range servers {
		go us.send(server)
	}
	<- us.Dying()
	return
}

func (us *UpStream) send(server string){
	for {
		conn, err := net.Dial("tcp", server)
		if err != nil {
			log.Printf("connect %s fail: %s", server, err)
			<- time.After(time.Duration(10) * time.Second)
			continue
		}
		defer conn.Close()
		select {
		case m := <- us.MailBox:
			buf, err := m.ToGob()
			if err != nil {
				log.Printf("encode metrics %s fail: %s", m.Name, err)
			}else{
				_, err := conn.Write(buf)
				if err != nil {
					log.Printf("send metrics %s fail: %s", m.Name, err)
					us.MailBox <- m
					conn.Close()
					<- time.After(time.Duration(10) * time.Second)
					continue
				}
			}
		case <- us.Dying():
			return
		}
	}
}


func (us *UpStream) Start(){
	err := us.Receive("json")
	if err != nil {
		log.Fatalf("start json receive fail: %s", err)
	}
	err = us.Receive("text")
	if err != nil {
		log.Fatalf("start text receive fail: %s", err)
	}

	err = us.Receive("gob")
	if err != nil {
		log.Fatalf("start gob receive fail: %s", err)
	}

	if context.GetString("cluster.mode") != "storeOnly"{
		us.Send()
	}
}


func (us *UpStream) Stop() error {
	us.Kill(nil)
	return us.Wait()
}
