package tcp

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"ringodis/interface/tcp"
	"ringodis/lib/logger"
	"sync"
	"syscall"
)

type Config struct {
	Address string
}

// 监听中断信号并通过 closeChan 通知服务器关闭
func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{})
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGHUP, syscall.SIGQUIT,
		syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigChan
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT,
			syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()

	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
	ListenAndServe(listener, handler, closeChan)
	return nil
}

// 监听并提供服务，在收到 closeChan 发来的关闭通知后关闭
func ListenAndServe(listener net.Listener, handler tcp.Handler,
	closeChan <-chan struct{}) {
	// 监听关闭的通知
	go func() {
		<-closeChan
		logger.Info("shutting down...")
		listener.Close() // 停止监听，listener.Accept()会立即返回io.EOF
		handler.Close()  // 关闭应用层服务器
	}()

	// 在异常退出后释放资源
	defer func() {
		listener.Close()
		handler.Close()
	}()
	ctx := context.Background()
	var waitDone sync.WaitGroup
	for {
		// 监听端口，阻塞直到收到新连接或者出现错误
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		// 开启一个新协程来处理新连接
		logger.Info("accepted link")
		waitDone.Add(1)
		go func() {
			defer func() {
				waitDone.Done()
			}()
			handler.Handle(ctx, conn)
		}()
	}
	waitDone.Wait()
}
