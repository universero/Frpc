package frpc

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

// methodType 代表一个RPC调用的方法类型
type methodType struct {
	// method 方法本身
	method reflect.Method
	// ArgType 入参类型
	ArgType reflect.Type
	// ReplyType 响应类型
	ReplyType reflect.Type
	// numCalls 用于统计调用次数
	numCalls uint64
}

func (m *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls)
}

// newArgv 创建入参
// 指针类型和值类型的创建有细节上的区别
func (m *methodType) newArgv() reflect.Value {
	var argv reflect.Value
	if m.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(m.ArgType.Elem())
	} else {
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

// newReplyv 创建响应
func (m *methodType) newReplyv() reflect.Value {
	replyv := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	default:
	}
	return replyv
}

// service 一个RPC服务
type service struct {
	// name 映射的结构体名称, 如WaitGroup
	name string
	// typ 结构体的类型
	typ reflect.Type
	// rcvr 结构体的实例本身
	rcvr reflect.Value
	// methods 存储映射的结构体的所有符合条件的方法
	method map[string]*methodType
}

func newService(rcvr interface{}) *service {
	s := new(service)
	s.rcvr = reflect.ValueOf(rcvr)
	s.name = reflect.Indirect(s.rcvr).Type().Name()
	s.typ = reflect.TypeOf(rcvr)
	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server: %s is not exported", s.name)
	}
	s.registerMethods()
	return s
}

// registerMethods 注册映射的对象的符合下列条件的方法
// 两个导出或内置类型的入参(反射时为3个,第0个为自身)
// 返回值只有一个1个, 类型为error
func (s *service) registerMethods() {
	s.method = make(map[string]*methodType)
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mType := method.Type
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argType, replyType := mType.In(1), mType.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		s.method[method.Name] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("registered method: %s.%s\n", s.name, method.Name)
	}
}

// isExportedOrBuiltinType 判断是否为导出类型或内置类型
func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}

// call 通过反射值调用方法
func (s *service) call(m *methodType, argv, replyv reflect.Value) error {
	atomic.AddUint64(&m.numCalls, 1)
	f := m.method.Func
	returnValues := f.Call([]reflect.Value{s.rcvr, argv, replyv})
	if errInter := returnValues[0].Interface(); errInter != nil {
		return errInter.(error)
	}
	return nil
}
