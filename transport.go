package simpleraft

import (
	"net"
	"time"
)

type SimpleTransport struct {
	addr     *net.TCPAddr
	listener net.Listener
}

func NewSimpleTransport(addr string) (*SimpleTransport, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &SimpleTransport{
		addr:     tcpAddr,
		listener: listener,
	}, nil
}

func (t *SimpleTransport) Accept() (net.Conn, error) {
	return t.listener.Accept()
}

func (t *SimpleTransport) Close() error {
	return t.listener.Close()
}

func (t *SimpleTransport) Addr() net.Addr {
	return t.addr
}

func (t *SimpleTransport) Dial(target string, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{
		Timeout: timeout,
	}
	return dialer.Dial("tcp", target)
}
