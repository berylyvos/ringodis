package cluster

import (
	"context"
	pool "github.com/jolestar/go-commons-pool/v2"
	"ringodis/config"
	database2 "ringodis/database"
	"ringodis/interface/database"
	"ringodis/interface/resp"
	"ringodis/lib/consistenthash"
)

type Cluster struct {
	self       string
	nodes      []string
	peerPicker *consistenthash.Map
	peerConn   map[string]*pool.ObjectPool
	db         database.DB
}

func MakeCluster() *Cluster {
	cluster := &Cluster{
		self:       config.Properties.Self,
		nodes:      make([]string, 0, len(config.Properties.Peers)+1),
		peerPicker: consistenthash.New(1, nil),
		peerConn:   make(map[string]*pool.ObjectPool),
		db:         database2.NewStandaloneServer(),
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

func (c *Cluster) Exec(client resp.Connection, cmdLine database.CmdLine) resp.Reply {
	//TODO implement me
	panic("implement me")
}

func (c *Cluster) Close() {
	//TODO implement me
	panic("implement me")
}

func (c *Cluster) AfterClientClose(client resp.Connection) {
	//TODO implement me
	panic("implement me")
}
