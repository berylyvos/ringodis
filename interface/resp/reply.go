package resp

// Reply is the interface of RESP(redis serialization protocol) message
type Reply interface {
	ToBytes() []byte
}
