package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

// GobCodec 是Gob类型的编解码器
type GobCodec struct {
	// 通过TCP或Unix建立的连接实例
	conn io.ReadWriteCloser
	// 防止阻塞的writer，用于提升性能
	buf *bufio.Writer
	// gob的编码和解码器
	dec *gob.Decoder
	enc *gob.Encoder
}

// 校验是否实现Codec接口
var _ Codec = (*GobCodec)(nil)

// NewGobCodec 根据conn构造一个GocCodec
func NewGobCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &GobCodec{
		conn: conn,
		buf:  buf,
		dec:  gob.NewDecoder(conn),
		enc:  gob.NewEncoder(conn),
	}
}

// ReadHeader 从buf中读取数据并存入header
func (c *GobCodec) ReadHeader(h *Header) error {
	return c.dec.Decode(h)
}

// ReadBody 从buf中读取数据并存入body
func (c *GobCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body)
}

// Write 写入Header和Body，并在结束时刷新缓冲区
func (c *GobCodec) Write(h *Header, body interface{}) (err error) {
	defer func() {
		_ = c.buf.Flush()
		if err != nil {
			_ = c.conn.Close()
		}
	}()
	if err := c.enc.Encode(h); err != nil {
		log.Println("rpc codec: gob error encoding header:", err)
		return err
	}
	if err := c.enc.Encode(body); err != nil {
		log.Println("rpc codec: gob error encoding body:", err)
		return err
	}
	return nil
}

// Close 关闭连接
func (c *GobCodec) Close() error {
	return c.conn.Close()
}
