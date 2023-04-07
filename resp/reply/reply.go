package reply

import (
	"bytes"
	"ringodis/interface/resp"
	"strconv"
)

var (
	CRLF = "\r\n"
)

/* ===== Bulk Reply ===== */

// BulkReply stores a binary-safe string
type BulkReply struct {
	Arg []byte
}

func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}

func (r *BulkReply) ToBytes() []byte {
	if r.Arg == nil {
		return nullBulkBytes
	}
	return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

/* ===== MultiBulk Reply ===== */

// MultiBulkReply stores a list of string
type MultiBulkReply struct {
	Args [][]byte
}

// MakeMultiBulkReply creates MultiBulkReply
func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Args: args,
	}
}

func (r *MultiBulkReply) ToBytes() []byte {
	argLen := len(r.Args)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.Write(nullBulkBytes)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}
	return buf.Bytes()
}

/* ===== Status Reply ===== */

// StatusReply stores a simple status string
type StatusReply struct {
	Status string
}

func MakeStatusReply(status string) *StatusReply {
	return &StatusReply{
		Status: status,
	}
}

func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

/* ===== Int Reply ===== */

// IntReply stores an int64 number
type IntReply struct {
	Code int64
}

func MakeIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

/* ===== Error Reply ===== */

// ErrorReply is an error and resp.Reply
type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

// StandardErrReply represents server error
type StandardErrReply struct {
	Status string
}

func (r StandardErrReply) Error() string {
	return r.Status
}

func (r StandardErrReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

func MakeErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

// IsErrorReply returns true if the given reply is an error
func IsErrorReply(reply resp.Reply) bool {
	return reply.ToBytes()[0] == '-'
}
