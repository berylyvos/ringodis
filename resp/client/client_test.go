package client

import (
	"ringodis/resp/reply"
	"testing"
)

func TestClient(t *testing.T) {
	client, err := MakeClient("localhost:6399")
	if err != nil {
		t.Error(err)
	}
	client.Start()

	result := client.Send([][]byte{
		[]byte("SET"),
		[]byte("server_name"),
		[]byte("ringodis"),
	})
	if statusRet, ok := result.(*reply.StatusReply); ok {
		if statusRet.Status != "OK" {
			t.Error("`set` failed, result: " + statusRet.Status)
		}
	}

	result = client.Send([][]byte{
		[]byte("GET"),
		[]byte("server_name"),
	})
	if bulkRet, ok := result.(*reply.BulkReply); ok {
		if string(bulkRet.Arg) != "ringodis" {
			t.Error("`get` failed, result: " + string(bulkRet.Arg))
		}
	}
}
