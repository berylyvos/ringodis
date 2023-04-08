package server

import (
	"context"
	"io"
	"net"
	"ringodis/database"
	idb "ringodis/interface/database"
	"ringodis/lib/logger"
	"ringodis/lib/sync/atomic"
	"ringodis/resp/conn"
	"ringodis/resp/parser"
	"ringodis/resp/reply"
	"strings"
	"sync"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

type Handler struct {
	activeConn sync.Map
	db         idb.DB
	closing    atomic.Boolean
}

func MakeHandler() *Handler {
	var db idb.DB
	db = database.NewEchoDB()
	return &Handler{
		db: db,
	}
}

// Handle receives and executes redis commands, writes result back to client. close connection when error occurs
func (h *Handler) Handle(ctx context.Context, netConn net.Conn) {
	if h.closing.Get() {
		_ = netConn.Close()
		return
	}

	client := conn.NewConn(netConn)
	h.activeConn.Store(client, struct{}{})

	ch := parser.ParseStream(netConn)
	for payload := range ch {
		if err := payload.Err; err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF ||
				strings.Contains(err.Error(), "use of closed network connection") {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			// protocol error
			errReply := reply.MakeErrReply(err.Error())
			if _, err = client.Write(errReply.ToBytes()); err != nil {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		if res := h.db.Exec(client, r.Args); res != nil {
			_, _ = client.Write(res.ToBytes())
		} else {
			_, _ = client.Write(unknownErrReplyBytes)
		}
	}
}

func (h *Handler) Close() error {
	logger.Info("ringodis handler shutting down")
	h.closing.Set(true)
	h.activeConn.Range(func(key, value any) bool {
		client := key.(*conn.Connection)
		_ = client.Close()
		return true
	})

	h.db.Close()
	return nil
}

func (h *Handler) closeClient(client *conn.Connection) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}
