package resp

// Connection represents a connection with redis client
type Connection interface {
	Write([]byte) (int, error)

	GetDBIndex() int
	SelectDB(int)
}
