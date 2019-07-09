package rocketmq

import (
	"encoding/json"
	"errors"
	"io"
)

const (
	cmdStart = iota
	cmdLength
	cmdEnd
)

// not for concurrent usage.
type cmdDecoder struct {
	r     io.Reader
	buf   []byte
	scanp int // start of unread data in buf

	tokenState   int
	length       int
	headerLength int
}

func (dec *cmdDecoder) refill() error {
	if dec.scanp > 0 {
		n := copy(dec.buf, dec.buf[dec.scanp:])
		dec.buf = dec.buf[:n]
		dec.scanp = 0
	}

	// Grow buffer if not large enough.
	const minRead = 512
	if cap(dec.buf)-len(dec.buf) < minRead {
		newBuf := make([]byte, len(dec.buf), 2*cap(dec.buf)+minRead)
		copy(newBuf, dec.buf)
		dec.buf = newBuf
	}

	// Read. Delay error for next iteration (after scan).
	n, err := dec.r.Read(dec.buf[len(dec.buf):cap(dec.buf)])
	dec.buf = dec.buf[0 : len(dec.buf)+n]
	return err
}

func (dec *cmdDecoder) readValue() error {
	var err error
	flag := true
	for flag {
		if err != nil {
			break
		}
		switch dec.tokenState {
		case cmdStart:
			if len(dec.buf[dec.scanp:]) < 4 {
				err = dec.refill()
			} else {
				dec.tokenState++
			}
		case cmdLength:
			length := varintInt(dec.buf[dec.scanp : dec.scanp+4])
			if len(dec.buf[dec.scanp+4:]) < length {
				err = dec.refill()
			} else {
				dec.length = dec.scanp + length + 4
				dec.tokenState++
			}
		case cmdEnd:
			headerLength := varintInt(dec.buf[dec.scanp+4 : dec.scanp+8])
			dec.headerLength = dec.scanp + 8 + headerLength
			flag = false
		}
	}

	return err
}

func (dec *cmdDecoder) Decode(cmd *RemotingCommand) error {

	if dec.tokenState != cmdStart {
		return errors.New("not at beginning of value")
	}

	var err error

	err = dec.readValue()
	if err != nil {
		return err
	}
	err = dec.unmarshal(cmd)
	dec.scanp = dec.length
	dec.length = 0
	dec.headerLength = 0
	dec.tokenState = cmdStart
	return err
}

func (dec *cmdDecoder) unmarshal(cmd *RemotingCommand) error {
	cmd.ExtFields = make(map[string]string)
	err := json.Unmarshal(dec.buf[dec.scanp+8:dec.headerLength], cmd)
	cmd.Body = make([]byte, dec.length-dec.headerLength)
	copy(cmd.Body, dec.buf[dec.headerLength:dec.length])
	return err
}

func newDecoder(r io.Reader) *cmdDecoder {
	return &cmdDecoder{r: r}
}
