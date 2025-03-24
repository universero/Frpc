package codec

import "io"

// Header 是rpc调用的头信息
type Header struct {
	ServiceMethod string // 形如 "Service.Method"
	Seq           uint64 // 客户端选择的序列号，唯一标识一个请求
	Error         error  // 错误信息，客户端置为空，发生错误时服务端设置
}

// Codec 是抽象出的编解码器接口，用于适配不同的编解码方式
type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

// NewCodecFunc 是一类创建Codec的方法，用于实现工厂方法
type NewCodecFunc func(io.ReadWriteCloser) Codec

// Type 是自定义的类型，用于区分不同的编码方式
type Type string

// 定义编码方式常量，暂时只实现了gob
const (
	GobType  = "application/gob"  // Gob是Go语言特有的二进制序列化格式，专为高效传输Go数据类型设计
	JsonType = "application/json" // 代码里目前没有实现
)

var NewCodecFuncMap map[Type]NewCodecFunc

// init 初始化了已有的编解码方式的构造函数
func init() {
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
}
