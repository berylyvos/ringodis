package tcp

import (
	"bufio"
	"context"
	"io"
	"net"
	"ringodis/lib/logger"
	"ringodis/lib/sync/atomic"
	"ringodis/lib/sync/wait"
	"sync"
	"time"
)

// 客户端连接的抽象
type Client struct {
	Conn net.Conn // tcp连接

	// 当服务端开始发送数据时进入waiting，阻止其他协程关闭连接
	// wait.wait封装了WaitGroup，有最大等待时间
	Waiting wait.Wait
}

type EchoHandler struct {
	// 保存所有工作状态client的集合（把map当set用）
	// 需使用并发安全的容器
	activeConn sync.Map

	// 关闭状态标识位
	closing atomic.Boolean
}

func MakeHandler() *EchoHandler {
	return &EchoHandler{}
}

func (handler *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	// 关闭中的handler不再处理新连接
	if handler.closing.Get() {
		_ = conn.Close()
		return
	}
	client := &Client{
		Conn: conn,
	}
	// 存储依然存活的连接
	handler.activeConn.Store(client, struct{}{})

	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("connection close")
				handler.activeConn.Delete(client)
			} else {
				logger.Warn(err)
			}
			return
		}
		// 发送数据前先置为waiting，阻止连接被关闭
		client.Waiting.Add(1)

		// 回送数据
		conn.Write([]byte(msg))
		client.Waiting.Done()
	}
}

// 关闭服务器
func (handler *EchoHandler) Close() error {
	logger.Info("handler shutting down...")
	handler.closing.Set(true)
	// 逐个关闭客户端连接
	handler.activeConn.Range(func(key, value any) bool {
		client := key.(*Client)
		_ = client.Conn.Close()
		return true
	})
	return nil
}

// 关闭客户端连接
func (c *Client) Close() error {
	c.Waiting.WaitWithTimeout(10 * time.Second)
	c.Conn.Close()
	return nil
}
