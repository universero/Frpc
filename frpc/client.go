package frpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/univero/frpc/codec"
	"io"
	"log"
	"net"
	"sync"
)

// Call 是一个RPC调用
type Call struct {
	// 唯一的请求标识
	Seq uint64

	// 存根，形如 "<service>.<method>"
	ServiceMethod string

	// 本次调用的参数
	Args any

	// 调用的响应
	Reply interface{}

	// 错误信息
	Error error

	// 调用结束时，会调用call.done()，用于支持异步
	Done chan *Call
}

// done 将call的指针存入chan，由于通知调用结束
func (c *Call) done() {
	c.Done <- c
}

// Client 是一个RPC客户端
// 一个Client可以有多个未完成的Call
// Client也可能被多个goroutine同时使用
type Client struct {
	// cc 编解码器
	cc codec.Codec

	// opt 配置项
	opt *Option

	// sending 发送互斥锁，保证请求的有效发送
	sending sync.Mutex

	// header 请求头，一个客户端只需要一个
	header codec.Header

	// mu 互斥锁, 确保Client操作的原子性
	mu sync.Mutex

	// seq 请求的唯一标识
	seq uint64

	// 未完成的请求, seq - Call
	pending map[uint64]*Call

	// closing 和 shutdown 任一值为true标识Client不可用
	// closing 是用户调用Close()主动关闭
	// shutdown 为有错误发生
	closing  bool
	shutdown bool
}

var _ io.Closer = (*Client)(nil)

var ErrShutDown = errors.New("frpc: client is shut down")

// Close 关闭客户端, 如果调用时已经closing了，则抛出异常
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closing {
		return ErrShutDown
	}
	c.closing = true
	return c.cc.Close()
}

// IsAvailable 判断客户端是否可用
func (c *Client) IsAvailable() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closing && !c.shutdown
}

// registerCall 注册一个Call, 将Call存入pending并更新c.seq
func (c *Client) registerCall(call *Call) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closing || c.shutdown {
		return 0, ErrShutDown
	}
	call.Seq = c.seq
	c.pending[call.Seq] = call
	c.seq++
	return call.Seq, nil
}

// removeCall 根据seq, 从c.seq移除对应的Call, 并返回
func (c *Client) removeCall(seq uint64) *Call {
	c.mu.Lock()
	defer c.mu.Unlock()
	call := c.pending[seq]
	delete(c.pending, seq)
	return call
}

// terminateCall 服务端和客户端发生错误调用, 将shutdown设置为true, 并将错误信息通知所有pending状态的Call
func (c *Client) terminateCall(err error) {
	c.sending.Lock()
	defer c.sending.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()

	c.shutdown = true
	for _, call := range c.pending {
		call.Error = err
		call.done()
	}
}

// NewClient 创建客户端
func NewClient(conn io.ReadWriteCloser, opt *Option) (*Client, error) {
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("codec %s not supported", opt.CodecType)
		log.Println(err)
		return nil, err
	}
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options err:", err)
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(f(conn), opt), nil
}

// newClientCodec 执行实际上的client创建
func newClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		seq:     1,
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	go client.receive()
	return client
}

// receive 接受请求
// 对于一个客户端来说, Call有三种情况
// Call不存在, 请求没有发送完整, 或因为其他原因被取消, 但服务端仍然处理
// Call存在, 但服务端处理出错, 即h.Error不为空
// call存在, 且服务端处理正常, 则从body中读取reply的值
func (c *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = c.cc.ReadHeader(&h); err != nil {
			break
		}
		call := c.removeCall(h.Seq)
		switch {
		case call == nil:
			// Call不存在
			err = c.cc.ReadBody(nil)
		case h.Error != nil:
			// Call存在但出错
			call.Error = fmt.Errorf("frpc: header error: %v", h.Error)
			err = c.cc.ReadBody(nil)
			call.done()
		default:
			err = c.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = fmt.Errorf("frpc: read body error: %v", h.Error)
			}
			call.done()
		}
	}
	c.terminateCall(err)
}

// parseOptions 解析配置项, 通过...将opts作为可选参数
func parseOptions(opts ...*Option) (*Option, error) {
	// 未指定则使用默认的
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

// Dial 在指定network上建立连接
func Dial(network, addr string, opts ...*Option) (c *Client, err error) {
	// 解析配置项
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	// 建立连接
	conn, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}

	// 创建失败则关闭连接
	defer func() {
		if c == nil {
			_ = conn.Close()
		}
	}()
	return NewClient(conn, opt)
}

// send 发送请求
func (c *Client) send(call *Call) {
	// 确保请求发送的有序性
	c.sending.Lock()
	defer c.sending.Unlock()

	// 注册Call
	seq, err := c.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	// 初始化请求头
	c.header.ServiceMethod = call.ServiceMethod
	c.header.Seq = seq
	c.header.Error = nil

	// 写入请求
	if err := c.cc.Write(&c.header, call.Args); err != nil {
		call := c.removeCall(call.Seq)
		// call 为nil说明写入请求失败
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// Go 异步的执行RPC调用, 完成时返回了代表本次调用的Call对象
// 异步实现原理: 创建client时会启用一个goroutine执行receive方法, 接受到Call时会写入对应的chanel
// 如果等待chan的写入则是同步的,不等待由于写入后就返回了所以是异步的
func (c *Client) Go(serviceMethod string, args interface{}, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 1)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	c.send(call)
	return call
}

// Call 同步的执行RPC调用, 并返回错误响应
func (c *Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	call := <-c.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
}
