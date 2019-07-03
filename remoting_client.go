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
	done            chan struct{}
}

type RemotingClient interface {
	RegisterResponse(opaque int32, callback InvokeCallback)
	InvokeAsync(addr string, request *RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error
	InvokeSync(addr string, request *RemotingCommand, timeoutMillis int64) (*RemotingCommand, error)
	InvokeOneway(addr string, request *RemotingCommand, timeoutMillis int64) error
	Start()
	Shutdown()
}

type DefaultRemotingClient struct {
	connTable     map[string]net.Conn
	connTableLock sync.RWMutex
	responseTable sync.Map
	running       bool
	m             sync.Mutex
	ch            chan struct{}
	wg            sync.WaitGroup
}

func NewDefaultRemotingClient() RemotingClient {
	return &DefaultRemotingClient{
		connTable: make(map[string]net.Conn),
	}
}

func (d *DefaultRemotingClient) RegisterResponse(opaque int32, callback InvokeCallback) {
	response := &ResponseFuture{
		sendRequestOK:  false,
		opaque:         opaque,
		beginTimestamp: time.Now().Unix(),
		invokeCallback: callback,
	}
	d.responseTable.Store(opaque, response)
}

func (d *DefaultRemotingClient) Start() {
	d.m.Lock()
	defer d.m.Unlock()
	d.running = true
	d.ch = make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		flag := true
		for flag {
			select {
			case <-ticker.C:
				if !d.running {
					flag = false
				}
				d.scanResponseTable()
			case <-d.ch:
				flag = false
			}
		}

	}()
}
func (d *DefaultRemotingClient) Shutdown() {
	d.m.Lock()
	defer d.m.Unlock()
	d.running = false
	close(d.ch)
	d.wg.Wait()            // wait all callback finish
	d.releaseAll()         // release connection
	d.cleanResponseTable() // deal with left response.
}
func (d *DefaultRemotingClient) scanResponseTable() {

	var toRemove []*ResponseFuture
	now := time.Now().Unix()
	d.responseTable.Range(func(key, value interface{}) bool {
		if value.(*ResponseFuture).beginTimestamp+value.(*ResponseFuture).timeoutMillis/1000 <= now && value.(*ResponseFuture).opaque > 1000 {
			toRemove = append(toRemove, value.(*ResponseFuture))
			d.responseTable.Delete(key)
		}
		return true
	})

	for _, response := range toRemove {
		if response.invokeCallback != nil {
			response.err = ErrTimeout
			response.invokeCallback(response)
			logger.Infof("remove time out request %v", response)
		}
	}

}

// TODO remove to mq_client
func (d *DefaultRemotingClient) newNameSrvConn(addr string) (conn net.Conn, err error) {
	d.connTableLock.Lock()
	defer d.connTableLock.Unlock()
	for _, address := range strings.Split(addr, ";") {
		conn, ok := d.connTable[address]
		if ok {
			return conn, nil
		}
		conn, err = net.Dial("tcp", address)
		if err != nil {
			continue
		}
		d.connTable[address] = conn
		go d.handleConn(conn, address)
		return conn, nil
	}
	if err == nil {
		err = connectErr(addr)
	}

	return nil, err

}
func (d *DefaultRemotingClient) connect(addr string) (conn net.Conn, err error) {
	if !d.running {
		return conn, errors.New("client is in shutdown process, stop new connection")
	}
	if strings.Contains(addr, ";") {
		return d.newNameSrvConn(addr)
	}

	d.connTableLock.RLock()
	conn, ok := d.connTable[addr]
	d.connTableLock.RUnlock()
	if !ok {
		newConn, err := net.Dial("tcp", addr)
		if err != nil {
			logger.Error(err)
			return nil, err
		}
		d.connTableLock.Lock()
		defer d.connTableLock.Unlock()
		conn, ok = d.connTable[addr]
		if !ok {
			d.connTable[addr] = newConn
			go d.handleConn(newConn, addr)
			conn = newConn
		}
	}
	return conn, nil
}

func (d *DefaultRemotingClient) InvokeSync(addr string, request *RemotingCommand, timeoutMillis int64) (*RemotingCommand, error) {
	conn, err := d.connect(addr)
	if err != nil {
		return nil, err
	}

	if request == nil {
		return nil, errors.New("nil request")
	}

	response := &ResponseFuture{
		sendRequestOK:  false,
		opaque:         request.Opaque,
		timeoutMillis:  timeoutMillis,
		beginTimestamp: time.Now().Unix(),
		done:           make(chan struct{}),
	}

	d.responseTable.Store(request.Opaque, response)

	err = d.sendRequest(request, conn, addr)
	if err != nil {
		logger.Error("invokeSync:err", err)
		return nil, err
	}
	select {
	case <-response.done:
		return response.responseCommand, nil
	case <-time.After(time.Duration(timeoutMillis) * time.Millisecond):
		return nil, errors.New("invoke sync timeout")
	}

}
func (d *DefaultRemotingClient) InvokeAsync(addr string, request *RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) (err error) {
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

	d.responseTable.Store(request.Opaque, response)

	err = d.sendRequest(request, conn, addr)
	if err != nil {
		logger.Error(err)
		return err
	}
	return nil
}
func (d *DefaultRemotingClient) InvokeOneway(addr string, request *RemotingCommand, timeoutMillis int64) (err error) {

	conn, err := d.connect(addr)
	if err != nil {
		logger.Error(err)
		return err
	}

	request.markOneWayRPC()

	return d.sendRequest(request, conn, addr)
}

func (d *DefaultRemotingClient) handleResponse(header, body []byte) {
	defer d.wg.Done()

	cmd := decodeRemoteCommand(header, body)
	logger.Debug("Received response:", cmd)
	resp, ok := d.responseTable.Load(cmd.Opaque)
	d.responseTable.Delete(cmd.Opaque)
	if ok {
		response := resp.(*ResponseFuture)
		response.responseCommand = cmd
		if response.invokeCallback != nil {
			response.invokeCallback(response)
		}

		if response.done != nil {
			close(response.done)
		}
	} else {
		if d.running {
			resp, ok := d.responseTable.Load(int32(cmd.Code))
			if ok {
				response := resp.(*ResponseFuture)
				response.responseCommand = cmd
				if response.invokeCallback != nil {
					response.invokeCallback(response)
				}
			} else {
				jsonCmd, err := json.Marshal(cmd)

				if err != nil {
					logger.Error(err)
				}
				logger.Info(string(jsonCmd))
			}
		}

	}
}

func (d *DefaultRemotingClient) handleConn(conn net.Conn, addr string) {
	b := make([]byte, 1024)
	var length, headerLength, bodyLength int32
	var buf = bytes.NewBuffer([]byte{})
	var header, body []byte
	//decoder := json.NewDecoder(conn)
	var flag = 0
	for {
		//conn.SetReadDeadline(time.Now().Add(time.Second * 10))
		// after stop send heartbeat will
		//decoder := json.NewDecoder(conn)
		//err := decoder.Decode(v)
		//if err != nil {
		//
		//}
		//cmd := new(RemotingCommand)
		//err := decoder.Decode(cmd)
		n, err := conn.Read(b)
		if err != nil {
			d.releaseConn(addr, conn)
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
				d.wg.Add(1)
				go d.handleResponse(headerCopy, bodyCopy)
			}
		}

	}
}
func (d *DefaultRemotingClient) sendRequest(request *RemotingCommand, conn net.Conn, addr string) error {
	logger.Debug("Send Request:", request)
	data, err := request.encode()
	if err != nil {
		return err
	}
	_, err = conn.Write(data)
	if err != nil {
		d.releaseConn(addr, conn)
		return err
	}
	return nil
}
func (d *DefaultRemotingClient) releaseAll() {
	d.connTableLock.Lock()
	defer d.connTableLock.Unlock()
	for _, conn := range d.connTable {
		conn.Close()
	}
}
func (d *DefaultRemotingClient) cleanResponseTable() {
	//// there should be no other goroutine access, but still check for sure.
	//d.responseTableLock.Lock()
	//defer d.responseTableLock.Unlock()
	//for _, r := range d.fifoQueue {
	//	if r.invokeCallback != nil {
	//		r.err = errors.New("server has bean shutdown")
	//		r.invokeCallback(r)
	//	}
	//}
}
func (d *DefaultRemotingClient) releaseConn(addr string, conn net.Conn) {
	d.connTableLock.Lock()
	conn.Close()
	delete(d.connTable, addr)
	d.connTableLock.Unlock()
}
