package cluster

import (
	"context"
	pool "github.com/jolestar/go-commons-pool/v2"
	"ringodis/config"
	"ringodis/database"
	idb "ringodis/interface/database"
	"ringodis/interface/resp"
	"ringodis/lib/consistenthash"
	"ringodis/lib/logger"
	"ringodis/resp/reply"
	"strings"
)

type Cluster struct {
	self       string
	nodes      []string
	peerPicker *consistenthash.Map
	peerConn   map[string]*pool.ObjectPool
	db         idb.DB
}

func (cluster *Cluster) Exec(client resp.Connection, cmdLine idb.CmdLine) (res resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
			res = reply.MakeUnknownErrReply()
		}
	}()
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmdFunc, ok := router[cmdName]
	if !ok {
		res = reply.MakeErrReply("not supported command")
	}
	res = cmdFunc(cluster, client, cmdLine)
	return
}

func (cluster *Cluster) Close() {
	cluster.db.Close()
}

func (cluster *Cluster) AfterClientClose(c resp.Connection) {
	cluster.db.AfterClientClose(c)
}

func MakeCluster() *Cluster {
	cluster := &Cluster{
		self:       config.Properties.Self,
		nodes:      make([]string, 0, len(config.Properties.Peers)+1),
		peerPicker: consistenthash.New(1, nil),
		peerConn:   make(map[string]*pool.ObjectPool),
		db:         database.NewStandaloneServer(),
	}
	ctx := context.Background()
	for _, peer := range config.Properties.Peers {
		cluster.nodes = append(cluster.nodes, peer)
		cluster.peerConn[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connFactory{
			Peer: peer,
		})
	}
	cluster.nodes = append(cluster.nodes, cluster.self)
	cluster.peerPicker.AddNode(cluster.nodes...)
	return cluster
}

type CmdFunc func(cluster *Cluster, c resp.Connection, cmdLine [][]byte) resp.Reply