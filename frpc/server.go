package frpc

import (
	"encoding/json"
	"errors"
	"github.com/univero/frpc/codec"
	"io"
	"log"
	"net"
	"reflect"
	"strings"
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
type Server struct {
	serviceMap sync.Map
}

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
	mType  *methodType
	serv   *service
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
	// 读取body
	req.serv, req.mType, err = s.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	req.argv = req.mType.newArgv()
	req.replyv = req.mType.newReplyv()

	// argvi 需要是一个指针
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}
	// 将请求报文反序列化为一个入参
	if err := cc.ReadBody(argvi); err != nil {
		log.Println("rpc server: read argv error", err)
		return req, err
	}
	return req, nil
}

func (s *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: send response error", err)
	}
}

// handleRequest 处理一个请求
func (s *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	err := req.serv.call(req.mType, req.argv, req.replyv)
	if err != nil {
		req.h.Error = err
		s.sendResponse(cc, req.h, invalidRequest, sending)
	}
	s.sendResponse(cc, req.h, req.replyv.Interface(), sending)
}

// Register 将一个类型注册到rpc服务中
func (s *Server) Register(rcvr interface{}) error {
	serv := newService(rcvr)
	if _, dip := s.serviceMap.LoadOrStore(serv.name, serv); dip {
		return errors.New("rpc server: service already defined, " + serv.name)
	}
	return nil
}

// Register 使用默认server注册
func Register(rcvr interface{}) error {
	return DefaultServer.Register(rcvr)
}

// 根据<service>.<method>找到service中对应的method
func (s *Server) findService(serviceMethod string) (serv *service, mType *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	servName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	servi, ok := s.serviceMap.Load(servName)
	if !ok {
		err = errors.New("rpc server: can't find service: " + serviceMethod)
	}
	serv = servi.(*service)
	mType = serv.method[methodName]
	if mType == nil {
		err = errors.New("rpc server: can't find method: " + serviceMethod)
	}
	return
}
