package cluster

import (
	"context"
	"errors"
	"ringodis/interface/resp"
	"ringodis/lib/utils"
	"ringodis/resp/client"
	"ringodis/resp/reply"
	"strconv"
)

func (cluster *Cluster) relay(peer string, conn resp.Connection, cmdLine [][]byte) resp.Reply {
	if peer == cluster.self {
		return cluster.db.Exec(conn, cmdLine)
	}
	peerClient, err := cluster.getPeerClient(peer)
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cluster.returnPeerClient(peer, peerClient)
	}()
	peerClient.Send(utils.ToCmdLine("SELECT", strconv.Itoa(conn.GetDBIndex())))
	return peerClient.Send(cmdLine)
}

func (cluster *Cluster) broadcast(conn resp.Connection, cmdLine [][]byte) map[string]resp.Reply {
	res := make(map[string]resp.Reply)
	for _, node := range cluster.nodes {
		res[node] = cluster.relay(node, conn, cmdLine)
	}
	return res
}

func (cluster *Cluster) getPeerClient(peer string) (*client.Client, error) {
	pool, ok := cluster.peerConn[peer]
	if !ok {
		return nil, errors.New("peer connection not found")
	}
	object, err := pool.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}
	c, ok := object.(*client.Client)
	if !ok {
		return nil, errors.New("type mismatch")
	}
	return c, nil
}

func (cluster *Cluster) returnPeerClient(peer string, c *client.Client) error {
	pool, ok := cluster.peerConn[peer]
	if !ok {
		return errors.New("peer connection not found")
	}
	return pool.ReturnObject(context.Background(), c)
}
