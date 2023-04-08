package server

import (
	"bufio"
	"fmt"
	"net"
	"ringodis/tcp"
	"testing"
	"time"
)

const (
	cmd  = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	cmd2 = "*3\r\n$3\r\nSET\r\n$5\r\nk\r\ney\r\n$8\r\nringodis\r\n"
)

func TestListenAndServe(t *testing.T) {
	var err error
	closeChan := make(chan struct{})
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Error(err)
		return
	}
	addr := listener.Addr().String()
	go tcp.ListenAndServe(listener, MakeHandler(), closeChan)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = conn.Write([]byte(cmd2))
	if err != nil {
		t.Error(err)
		return
	}
	bufReader := bufio.NewReader(conn)

	res := ""
	for i := 0; i < 8; i++ {
		line, _, err := bufReader.ReadLine()
		if err != nil {
			t.Error(err)
			return
		}
		fmt.Println(string(line))
		res += string(line) + "\r\n"
	}

	if res != cmd2 {
		t.Error("wrong response")
		return
	}
	closeChan <- struct{}{}
	time.Sleep(time.Second)
}
