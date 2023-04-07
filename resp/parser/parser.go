package parser

import (
	"bufio"
	"errors"
	"io"
	"ringodis/interface/resp"
	"ringodis/lib/logger"
	"ringodis/resp/reply"
	"runtime/debug"
	"strconv"
)

const pErr = "protocol error: "

// Payload stores resp.Reply or error
type Payload struct {
	Data resp.Reply
	Err  error
}

type readState struct {
	readingMultiLine  bool
	expectedArgsCount int
	msgType           byte
	args              [][]byte
	bulkLen           int64
}

func makeReadState() *readState {
	return new(readState)
}

func (s *readState) reset() {
	s.readingMultiLine = false
	s.expectedArgsCount = 0
	s.msgType = 0
	s.args = nil
	s.bulkLen = 0
}

func (s *readState) parsed() bool {
	return s.expectedArgsCount > 0 && s.expectedArgsCount == len(s.args)
}

// ParseStream reads data from io.Reader and send payloads through channel
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

// parse0 is the main parser
func parse0(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()

	br := bufio.NewReader(reader)
	state := makeReadState()
	var msg []byte
	var err error
	for {
		var ioErr bool
		msg, ioErr, err = readLine(br, state)
		if err != nil {
			if ioErr {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
			ch <- &Payload{Err: err}
			state.reset()
			continue
		}

		if state.readingMultiLine {
			if err = parseBody(msg, state); err != nil {
				ch <- &Payload{Err: err}
			} else if state.parsed() {
				var rep resp.Reply
				if state.msgType == '*' {
					rep = reply.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					rep = reply.MakeBulkReply(state.args[0])
				}
				ch <- &Payload{
					Data: rep,
					Err:  err,
				}
			}
		} else {
			if msg[0] == '*' {
				if err = parseMultiBulkHeader(msg, state); err != nil {
					ch <- &Payload{Err: err}
				} else if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: reply.MakeEmptyMultiBulkReply(),
					}
				}
			} else if msg[0] == '$' {
				if err = parseBulkHeader(msg, state); err != nil {
					ch <- &Payload{Err: err}
				} else if state.bulkLen == -1 {
					ch <- &Payload{
						Data: reply.MakeNullBulkReply(),
					}
				}
			} else { // msg[0] == '+' || '-' || ':'
				rep, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: rep,
					Err:  err,
				}
			}
		}
		state.reset()
	}
}

// readLine read a single line from io.Reader into bytes
func readLine(br *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error

	if state.bulkLen == 0 {
		// if there's no '$', split by "\r\n"
		if msg, err = br.ReadBytes('\n'); err != nil {
			return nil, true, err
		}
		if n := len(msg); n == 0 || msg[n-2] != '\r' {
			return nil, false, errors.New(pErr + string(msg))
		}
	} else {
		// starting with '$n', just read n + 2 bytes
		msg = make([]byte, state.bulkLen+2)
		if _, err = io.ReadFull(br, msg); err != nil {
			return nil, true, err
		}
		if n := len(msg); n == 0 || msg[n-2] != '\r' || msg[n-1] != '\n' {
			return nil, false, errors.New(pErr + string(msg))
		}
		state.bulkLen = 0 // reset bulk length for reading next line
	}

	return msg, false, nil
}

// parseMultiBulkHeader parse a multi bulk header (e.g. "*3\r\n...")
func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64
	if expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 64); err != nil {
		return errors.New(pErr + string(msg))
	}
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = int(expectedLine)
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return errors.New(pErr + string(msg))
	}
}

// parseBulkHeader parse a single bulk header (e.g. "$4\r\nPING\r\n")
func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	if state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64); err != nil {
		return errors.New(pErr + string(msg))
	}
	if state.bulkLen == -1 {
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New(pErr + string(msg))
	}
}

// parseSingleLineReply parse single line (e.g. "+OK\r\n", ":42\r\n") and return a resp.Reply
func parseSingleLineReply(msg []byte) (resp.Reply, error) {
	line := string(msg[:len(msg)-2])
	var res resp.Reply
	switch msg[0] {
	case '+':
		res = reply.MakeStatusReply(line[1:])
	case '-':
		res = reply.MakeErrReply(line[1:])
	case ':':
		if val, err := strconv.ParseInt(line[1:], 10, 64); err != nil {
			return nil, errors.New(pErr + string(msg))
		} else {
			res = reply.MakeIntReply(val)
		}
	}
	return res, nil
}

// parseBody read the non-first lines of multi bulk reply or bulk reply
func parseBody(msg []byte, state *readState) error {
	line := msg[0 : len(msg)-2]
	var err error
	if line[0] == '$' {
		if state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64); err != nil {
			return errors.New(pErr + string(msg))
		}
		if state.bulkLen <= 0 {
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}
