package rocketmq

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync"
	"time"
)

type InvokeCallback func(responseFuture *ResponseFuture)

type ResponseFuture struct {
	responseCommand *RemotingCommand
	sendRequestOK   bool
	err             error
	opaque          int32
	timeoutMillis   int64
	invokeCallback  InvokeCallback
	beginTimestamp  int64
	done            chan bool
}

type RemotingClient interface {
	connect(addr string) (net.Conn, error)
	invokeAsync(addr string, request *RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error
	invokeSync(addr string, request *RemotingCommand, timeoutMillis int64) (*RemotingCommand, error)
	invokeOneway(addr string, request *RemotingCommand, timeoutMillis int64) error
	ScanResponseTable()
}

type DefaultRemotingClient struct {
	connTable         map[string]net.Conn
	connTableLock     sync.RWMutex
	responseTable     map[int32]*ResponseFuture
	responseTableLock sync.RWMutex
}

func NewDefaultRemotingClient() RemotingClient {
	return &DefaultRemotingClient{
		connTable:     make(map[string]net.Conn),
		responseTable: make(map[int32]*ResponseFuture),
	}
}

func (d *DefaultRemotingClient) ScanResponseTable() {
	d.responseTableLock.Lock()
	for seq, response := range d.responseTable {
		if (response.beginTimestamp + 30) <= time.Now().Unix() {
			delete(d.responseTable, seq)
			if response.invokeCallback != nil {
				response.invokeCallback(nil)
				logger.Infof("remove time out request %v", response)
			}
		}
	}
	d.responseTableLock.Unlock()

}
func (d *DefaultRemotingClient) newNameSrvConn(addr string) (conn net.Conn, err error) {
	d.connTableLock.Lock()
	defer d.connTableLock.Unlock()
	for _, address := range strings.Split(addr, ";") {
		conn, ok := d.connTable[address]
		if ok {
			return conn, nil
		} else {
			conn, err = net.Dial("tcp", address)
			if err != nil {
				logger.Error(err)
				break
			}
			d.connTable[address] = conn
			logger.Info("connect to:", address)
			go d.handlerConn(conn, address)
			return conn, nil
		}
	}

	return nil, errors.New("no available connection to namesrv")

}

func (d *DefaultRemotingClient) connect(addr string) (conn net.Conn, err error) {
	if strings.Contains(addr, ";") {
		return d.newNameSrvConn(addr)
	}

	d.connTableLock.Lock()
	defer d.connTableLock.Unlock()

	conn, ok := d.connTable[addr]

	if ok {
		return conn, nil
	} else {

		conn, err = net.Dial("tcp", addr)
		if err != nil {
			logger.Error(err)
			return nil, err
		}

		d.connTable[addr] = conn
		logger.Info("connect to:", addr)
		go d.handlerConn(conn, addr)
	}
	return conn, nil
}

func (d *DefaultRemotingClient) invokeSync(addr string, request *RemotingCommand, timeoutMillis int64) (*RemotingCommand, error) {
	conn, err := d.connect(addr)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	response := &ResponseFuture{
		sendRequestOK:  false,
		opaque:         request.Opaque,
		timeoutMillis:  timeoutMillis,
		beginTimestamp: time.Now().Unix(),
		done:           make(chan bool),
	}

	header := request.encodeHeader()
	body := request.Body
	d.responseTableLock.Lock()
	d.responseTable[request.Opaque] = response
	d.responseTableLock.Unlock()

	err = d.sendRequest(header, body, conn, addr)
	if err != nil {
		logger.Error("invokeSync:err", err)
		return nil, err
	}
	select {
	case <-response.done:
		return response.responseCommand, nil
	case <-time.After(3 * time.Second):
		return nil, errors.New("invoke sync timeout")
	}

}

func (d *DefaultRemotingClient) invokeAsync(addr string, request *RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) (err error) {
	conn, err := d.connect(addr)
	if err != nil {
		logger.Error(err)
		return err
	}

	response := &ResponseFuture{
		sendRequestOK:  false,
		opaque:         request.Opaque,
		timeoutMillis:  timeoutMillis,
		beginTimestamp: time.Now().Unix(),
		invokeCallback: invokeCallback,
	}

	d.responseTableLock.Lock()
	d.responseTable[request.Opaque] = response
	d.responseTableLock.Unlock()

	header := request.encodeHeader()
	body := request.Body
	err = d.sendRequest(header, body, conn, addr)
	if err != nil {
		logger.Error(err)
		return err
	}
	return nil
}

func (d *DefaultRemotingClient) invokeOneway(addr string, request *RemotingCommand, timeoutMillis int64) (err error) {

	conn, err := d.connect(addr)
	if err != nil {
		logger.Error(err)
		return err
	}

	request.markOnewayRPC()
	header := request.encodeHeader()
	body := request.Body

	return d.sendRequest(header, body, conn, addr)
}

func (d *DefaultRemotingClient) handlerConn(conn net.Conn, addr string) {
	b := make([]byte, 1024)
	var length, headerLength, bodyLength int32
	var buf = bytes.NewBuffer([]byte{})
	var header, body []byte
	var flag = 0
	for {
		n, err := conn.Read(b)
		if err != nil {
			d.releaseConn(addr, conn)
			logger.Error(err, addr)
			return
		}

		_, err = buf.Write(b[:n])
		if err != nil {
			d.releaseConn(addr, conn)
			return
		}

		for {
			if flag == 0 {
				if buf.Len() >= 4 {
					err = binary.Read(buf, binary.BigEndian, &length)
					if err != nil {
						logger.Error(err)
						return
					}
					flag = 1
				} else {
					break
				}
			}

			if flag == 1 {
				if buf.Len() >= 4 {
					err = binary.Read(buf, binary.BigEndian, &headerLength)
					if err != nil {
						logger.Error(err)
						return
					}
					flag = 2
				} else {
					break
				}

			}

			if flag == 2 {
				if (buf.Len() > 0) && (buf.Len() >= int(headerLength)) {
					header = make([]byte, headerLength)
					_, err = buf.Read(header)
					if err != nil {
						logger.Error(err)
						return
					}
					flag = 3
				} else {
					break
				}
			}

			if flag == 3 {
				bodyLength = length - 4 - headerLength
				if bodyLength == 0 {
					flag = 0
				} else {

					if buf.Len() >= int(bodyLength) {
						body = make([]byte, int(bodyLength))
						_, err = buf.Read(body)
						if err != nil {
							logger.Error(err)
							return
						}
						flag = 0
					} else {
						break
					}
				}
			}

			if flag == 0 {
				headerCopy := make([]byte, len(header))
				bodyCopy := make([]byte, len(body))
				copy(headerCopy, header)
				copy(bodyCopy, body)
				go func() {
					cmd := decodeRemoteCommand(headerCopy, bodyCopy)
					d.responseTableLock.RLock()
					response, ok := d.responseTable[cmd.Opaque]
					d.responseTableLock.RUnlock()

					d.responseTableLock.Lock()
					delete(d.responseTable, cmd.Opaque)
					d.responseTableLock.Unlock()

					if ok {
						response.responseCommand = cmd
						if response.invokeCallback != nil {
							response.invokeCallback(response)
						}

						if response.done != nil {
							response.done <- true
						}
					} else {
						if cmd.Code == NotifyConsumerIdsChanged {
							return
						}
						jsonCmd, err := json.Marshal(cmd)

						if err != nil {
							logger.Error(err)
						}
						logger.Info(string(jsonCmd))
					}
				}()
			}
		}

	}
}

func (d *DefaultRemotingClient) sendRequest(header, body []byte, conn net.Conn, addr string) error {
	//log.Debug("send request to addr")
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, int32(len(header)+len(body)+4))
	binary.Write(buf, binary.BigEndian, int32(len(header)))
	binary.Write(buf, binary.BigEndian, header)
	if body != nil && len(body) > 0 {
		binary.Write(buf, binary.BigEndian, body)
	}
	_, err := conn.Write(buf.Bytes())
	if err != nil {
		d.releaseConn(addr, conn)
		return err
	}
	return nil
}

func (d *DefaultRemotingClient) releaseConn(addr string, conn net.Conn) {
	conn.Close()
	d.connTableLock.Lock()
	delete(d.connTable, addr)
	d.connTableLock.Unlock()
}
