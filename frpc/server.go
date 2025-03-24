package frpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/univero/frpc/codec"
	"io"
	"log"
	"net"
	"reflect"
	"sync"
)

// MagicNumber 是标识协议的"魔法数字", 这里选用的是FRPC的ASCII码表示
const MagicNumber = 0x46525043

// Option 配置项, 用于协商各类信息
type Option struct {
	// 表示frpc协议的magic number
	MagicNumber int
	// 使用的编解码类型
	CodecType codec.Type
}

// DefaultOption 默认配置项
var DefaultOption = &Option{
	MagicNumber: MagicNumber,
	CodecType:   codec.GobType,
}

// Server 是一个FRPC服务端
type Server struct{}

// NewServer 构造一个新的服务端
func NewServer() *Server {
	return &Server{}
}

// DefaultServer 是默认的FRPC服务端实例
var DefaultServer = NewServer()

// Accept 监听连接，并处理每个连接上的请求
func (s *Server) Accept(lis net.Listener) {
	for {
		// 获取一个连接
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server: accept error", err)
			return
		}
		// 处理一个连接
		go s.ServerConn(conn)
	}
}

// Accept 使用默认的Server处理每一个请求
func Accept(lis net.Listener) { DefaultServer.Accept(lis) }

// ServerConn 处理一个连接
func (s *Server) ServerConn(conn net.Conn) {
	defer func() {
		_ = conn.Close()
	}()

	// 解析可选项
	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: decode option error", err)
		return
	}
	// 判断协议类型是否符合
	if opt.MagicNumber != MagicNumber {
		log.Println("rpc server: invalid magic number", opt.MagicNumber)
		return
	}
	// 获取编码类型的构造函数
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Println("rpc server: invalid codec type", opt.CodecType)
		return
	}
	// 处理编解码器
	s.serverCodec(f(conn))
}

var invalidRequest = struct{}{}

// serverCodec 处理一个编解码器
func (s *Server) serverCodec(cc codec.Codec) {
	sending := new(sync.Mutex)
	defer sending.Unlock()
	wg := new(sync.WaitGroup) // wait until all request are handled

	for {
		// 从连接中读取请求
		req, err := s.readRequest(cc)
		if err != nil {
			if req == nil {
				break // it's not possible to recover, so close the connection
			}
			req.h.Error = err
			s.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		go s.handleRequest(cc, req, sending, wg)
	}

	// 等待所有请求完成，关闭连接
	wg.Wait()
	_ = cc.Close()
}

// request 是一次rpc请求与响应
type request struct {
	h      *codec.Header
	argv   reflect.Value
	replyv reflect.Value
}

// readRequestHeader 读取请求头
func (s *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
			log.Println("rpc server: read header error", err)
		}
		return nil, err
	}
	return &h, nil
}

// readRequest 读取一个请求
func (s *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := s.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}

	req := &request{h: h}
	// TODO: 目前argv的类型未知，先假设为string
	// 读取body
	req.argv = reflect.New(reflect.TypeOf(""))
	if err := cc.ReadBody(req.argv.Interface()); err != nil {
		log.Println("rpc server: read request body error", err)
	}

	return req, nil
}

// sendResponse 向调用方写入响应
func (s *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: send response error", err)
	}
}

// handleRequest 处理一个请求
func (s *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	// TODO, should call registered rpc methods to get the right replyv
	// day 1, just print argv and send a hello message
	log.Println(req.h, req.argv.Elem())
	req.replyv = reflect.ValueOf(fmt.Sprintf("frpc resp %d", req.h.Seq))
	s.sendResponse(cc, req.h, req.replyv.Interface(), sending)
}
