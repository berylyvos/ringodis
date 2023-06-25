package client

import (
	"errors"
	"net"
	"ringodis/interface/resp"
	"ringodis/lib/logger"
	"ringodis/lib/sync/wait"
	"ringodis/resp/parser"
	"ringodis/resp/reply"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Client is a pipeline mode redis client
type Client struct {
	conn        net.Conn
	addr        string
	pendingReqs chan *request // pending to send
	waitingReqs chan *request // waiting for response
	status      int32
	working     *sync.WaitGroup // wait for unfinished requests (pending & waiting)
}

const (
	created = iota
	running
	closed
)

type request struct {
	id      uint64
	args    [][]byte
	reply   resp.Reply
	waiting *wait.Wait
	err     error
}

const (
	chanSize = 256
	maxWait  = 3 * time.Second
)

func MakeClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:        conn,
		addr:        addr,
		pendingReqs: make(chan *request, chanSize),
		waitingReqs: make(chan *request, chanSize),
		working:     &sync.WaitGroup{},
	}, nil
}

func (client *Client) Start() {
	go client.handleWrite()
	go client.handleRead()
	atomic.StoreInt32(&client.status, running)
}

func (client *Client) Close() {
	atomic.StoreInt32(&client.status, closed)
	close(client.pendingReqs)
	client.working.Wait()
	_ = client.conn.Close()
	close(client.waitingReqs)
}

func (client *Client) reconnect() {
	logger.Info("reconnect with: " + client.addr)
	_ = client.conn.Close()

	var conn net.Conn
	for i := 0; i < 3; i++ {
		var err error
		conn, err = net.Dial("tcp", client.addr)
		if err != nil {
			logger.Error("reconnect error: " + err.Error())
			time.Sleep(time.Second)
			continue
		} else {
			break
		}
	}
	if conn == nil {
		client.Close()
		return
	}
	client.conn = conn
	close(client.waitingReqs)
	for req := range client.waitingReqs {
		req.err = errors.New("connection closed")
		req.waiting.Done()
	}
	client.waitingReqs = make(chan *request, chanSize)
	go client.handleRead()
}

func (client *Client) Send(args [][]byte) resp.Reply {
	if atomic.LoadInt32(&client.status) != running {
		return reply.MakeErrReply("client closed")
	}
	req := &request{
		args:    args,
		waiting: &wait.Wait{},
	}
	req.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	client.pendingReqs <- req
	if req.waiting.WaitWithTimeout(maxWait) {
		return reply.MakeErrReply("server time out")
	}
	if req.err != nil {
		return reply.MakeErrReply("request failed " + req.err.Error())
	}
	return req.reply
}

func (client *Client) handleWrite() {
	for req := range client.pendingReqs {
		client.doRequest(req)
	}
}

func (client *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}
	b := reply.MakeMultiBulkReply(req.args).ToBytes()
	var err error
	for i := 0; i < 3; i++ {
		_, err = client.conn.Write(b)
		if err == nil || (!strings.Contains(err.Error(), "timeout") &&
			!strings.Contains(err.Error(), "deadline exceeded")) {
			break
		}
	}
	if err == nil {
		client.waitingReqs <- req
	} else {
		req.err = err
		req.waiting.Done()
	}
}

func (client *Client) finishRequest(reply resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Error(err)
		}
	}()
	req := <-client.waitingReqs
	if req == nil {
		return
	}
	req.reply = reply
	if req.waiting != nil {
		req.waiting.Done()
	}
}

func (client *Client) handleRead() {
	ch := parser.ParseStream(client.conn)
	for payload := range ch {
		if payload.Err != nil {
			status := atomic.LoadInt32(&client.status)
			if status == closed {
				return
			}
			client.reconnect()
			return
		}
		client.finishRequest(payload.Data)
	}
}
