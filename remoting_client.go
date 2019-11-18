package rocketmq

import (
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

// 接收服务器主动发送的命令，无对应请求，目前唯一的使用是服务器发送订阅组新增consumer用来主动触发reblance.
func (d *DefaultRemotingClient) RegisterResponse(opaque int32, callback InvokeCallback) {
	if opaque >= 1000 || opaque <= 0 {
		return
	}
	response := &ResponseFuture{
		sendRequestOK:  false,
		opaque:         opaque,
		beginTimestamp: time.Now().Unix(),
		invokeCallback: callback,
		timeoutMillis:  -1,
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
	d.cleanResponseTable() // deal with left request.
}
func (d *DefaultRemotingClient) scanResponseTable() {

	var toRemove []*ResponseFuture
	now := time.Now().Unix()
	d.responseTable.Range(func(key, value interface{}) bool {
		rf := value.(*ResponseFuture)
		// 超时两倍之后移除
		if rf.timeoutMillis > 0 && rf.beginTimestamp+rf.timeoutMillis/1000*2 <= now {
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

// 只要有一个namesrv可用即可
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
			return nil, err
		}
		d.connTableLock.Lock()
		defer d.connTableLock.Unlock()
		conn, ok = d.connTable[addr]
		if !ok {
			d.connTable[addr] = newConn
			go d.handleConn(newConn, addr)
			conn = newConn
		} else {
			_ = newConn.Close()
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
		return nil, err
	}
	t := time.NewTimer(time.Duration(timeoutMillis) * time.Millisecond)
	defer func() { t.Stop() }()
	select {
	case <-response.done:
		return response.responseCommand, nil
	case <-t.C:
		return nil, errors.New("invoke sync timeout")
	}

}
func (d *DefaultRemotingClient) InvokeAsync(addr string, request *RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) (err error) {
	conn, err := d.connect(addr)
	if err != nil {
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
		return err
	}
	return nil
}
func (d *DefaultRemotingClient) InvokeOneway(addr string, request *RemotingCommand, timeoutMillis int64) (err error) {

	conn, err := d.connect(addr)
	if err != nil {
		return err
	}

	request.markOneWayRPC()

	return d.sendRequest(request, conn, addr)
}

func (d *DefaultRemotingClient) handleResponse(cmd *RemotingCommand) {
	defer d.wg.Done()
	logger.Debug("Received response:", cmd)
	resp, ok := d.responseTable.Load(cmd.Opaque)
	d.responseTable.Delete(cmd.Opaque)
	if ok {
		// 这里不需要判断running，即使在shutdown过程中也尽量完成正常请求
		response := resp.(*ResponseFuture)
		response.responseCommand = cmd
		if response.invokeCallback != nil {
			response.invokeCallback(response)
		}

		if response.done != nil {
			close(response.done)
		}
	} else {
		// 如果已经shutdown，服务器的命令也不需要关注。
		if d.running {
			resp, ok := d.responseTable.Load(int32(cmd.Code))
			if ok {
				response := resp.(*ResponseFuture)
				response.responseCommand = cmd
				if response.invokeCallback != nil {
					response.invokeCallback(response)
				}
			} else {
				logger.Infof("can't find callback for %v, maybe timeout or receive server command", cmd)
			}
		}

	}
}

func (d *DefaultRemotingClient) handleConn(conn net.Conn, addr string) {
	defer func() {
		// make sure release conn.
		d.releaseConn(addr, conn)
	}()
	decoder := newDecoder(conn)
	for {
		cmd := new(RemotingCommand)
		err := decoder.Decode(cmd)
		if err != nil {
			logger.Error("decode cmd fail:", err)
			return
		}
		d.wg.Add(1)
		go d.handleResponse(cmd)

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
	for _, conn := range d.connTable {
		_ = conn.Close()
	}
}

func (d *DefaultRemotingClient) cleanResponseTable() {
	d.responseTable.Range(func(key, value interface{}) bool {
		rf := value.(*ResponseFuture)
		if rf.opaque >= 1000 || rf.opaque < 0 {
			if rf.invokeCallback != nil {
				rf.err = ErrShutdown
				rf.invokeCallback(rf)
			}
		}
		return true
	})
}

func (d *DefaultRemotingClient) releaseConn(addr string, conn net.Conn) {
	d.connTableLock.Lock()
	_ = conn.Close()
	delete(d.connTable, addr)
	d.connTableLock.Unlock()
}
