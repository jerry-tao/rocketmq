package rocketmq

import (
	"errors"
	"io"
  "encoding/json"
)
const (
	cmdStart = iota
  cmdLength
  cmdEnd
)
type cmdDecoder struct{
	r       io.Reader
	buf     []byte
	scanp   int   // start of unread data in buf

	tokenState   int
  length       int
  headerLength int
}

func (dec *cmdDecoder) refill() error{
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

func (dec *cmdDecoder) readValue()(error) {
	scanp := dec.scanp
	var err error
End:
	for{
    if err!=nil{
      break
    }
    switch dec.tokenState {
    case cmdStart:
      if len(dec.buf[scanp:])<4{
        err  = dec.refill()
      }else{
        dec.tokenState++
      }
    case cmdLength:
        length := varintInt(dec.buf[scanp:scanp+4])
        if len(dec.buf[scanp:])<length{
          err = dec.refill()
        }else{
          dec.length = dec.scanp+length+4
          dec.tokenState++
        }
    case cmdEnd:
      headerLength := varintInt(dec.buf[scanp+4:scanp+8])
      dec.headerLength = dec.scanp+8+headerLength
      break End
    }
	}

	return err
}

func (dec *cmdDecoder)Decode(cmd *RemotingCommand) error{

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
	dec.tokenState = cmdStart
	return err
}

func (dec *cmdDecoder) unmarshal(cmd *RemotingCommand) error{
   cmd.ExtFields = make(map[string]string)
   err := json.Unmarshal(dec.buf[dec.scanp+8:dec.headerLength], cmd)
   cmd.Body = dec.buf[dec.headerLength:dec.length]
   return err
}

func newDecoder(r io.Reader) *cmdDecoder {
	return  &cmdDecoder{r: r}
}
