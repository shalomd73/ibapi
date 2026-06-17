package ibapi

import (
	"fmt"
	"net"
	"time"
)

const (
	maxReconnectAttempts = 3
	reconnectDelay       = 500 * time.Millisecond
)

// Connection is a TCPConn wrapper.
type Connection struct {
	*net.TCPConn
	host         string
	port         int
	isConnected  bool
	numBytesSent int
	numMsgSent   int
	numBytesRecv int
	numMsgRecv   int
}

func (c *Connection) Write(bs []byte) (int, error) {
	if c == nil {
		return 0, fmt.Errorf("ib connection is nil")
	}
	conn := c.TCPConn
	if conn == nil {
		if c.host == "" || c.port <= 0 {
			return 0, fmt.Errorf("ib tcp connection is nil")
		}
		log.Warn().Msg("Write attempted with nil TCP connection, attempting to reconnect...")
		if err := c.reconnect(); err != nil {
			return 0, fmt.Errorf("write failed and reconnection failed: %w", err)
		}
		conn = c.TCPConn
		if conn == nil {
			return 0, fmt.Errorf("write failed: tcp connection is still nil after reconnect")
		}
	}

	n, err := conn.Write(bs)
	if err != nil {
		c.isConnected = false
		log.Warn().Err(err).Msg("Write error detected, attempting to reconnect...")
		if err := c.reconnect(); err != nil {
			return 0, fmt.Errorf("write failed and reconnection failed: %w", err)
		}
		// Retry write after successful reconnection
		return c.TCPConn.Write(bs)
	}

	c.numBytesSent += n
	c.numMsgSent++

	log.Trace().Int("nBytes", n).Msg("conn write")

	return n, err
}

func (c *Connection) Read(bs []byte) (int, error) {
	if c == nil || c.TCPConn == nil {
		return 0, fmt.Errorf("ib tcp connection is nil")
	}
	n, err := c.TCPConn.Read(bs)

	c.numBytesRecv += n
	c.numMsgRecv++

	log.Trace().Int("nBytes", n).Msg("conn read")

	return n, err
}

func (c *Connection) reset() {
	c.numBytesSent = 0
	c.numBytesRecv = 0
	c.numMsgSent = 0
	c.numMsgRecv = 0
}

func (c *Connection) connect(host string, port int) error {
	c.host = host
	c.port = port
	c.reset()

	address := fmt.Sprintf("%v:%v", c.host, c.port)
	addr, err := net.ResolveTCPAddr("tcp4", address)
	if err != nil {
		log.Error().Err(err).Str("host", address).Msg("failed to resove tcp address")
		return err
	}

	c.TCPConn, err = net.DialTCP("tcp4", nil, addr)
	if err != nil {
		log.Error().Err(err).Any("address", addr).Msg("failed to dial tcp")
		return err
	}

	log.Debug().Any("address", c.TCPConn.RemoteAddr()).Msg("tcp socket connected")
	c.isConnected = true

	return nil
}

func (c *Connection) reconnect() error {
	attempts := 0
	backoff := reconnectDelay // Start with base delay

	for attempts < maxReconnectAttempts {
		log.Info().
			Int("attempt", attempts+1).
			Int("maxAttempts", maxReconnectAttempts).
			Msg("Attempting to reconnect")

		err := c.connect(c.host, c.port)
		if err == nil {
			log.Info().Msg("Reconnection successful")
			return nil
		}

		attempts++
		if attempts == maxReconnectAttempts {
			return fmt.Errorf("failed to reconnect after %d attempts", attempts)
		}

		time.Sleep(backoff)
		backoff *= 2 // Exponential backoff
	}

	return fmt.Errorf("failed to reconnect after %d attempts", attempts)
}

func (c *Connection) disconnect() error {
	if c == nil {
		return nil
	}
	log.Trace().
		Int("nMsgSent", c.numMsgSent).Int("nBytesSent", c.numBytesSent).
		Int("nMsgRecv", c.numMsgRecv).Int("nBytesRecv", c.numBytesRecv).
		Msg("conn disconnect")
	c.isConnected = false
	if c.TCPConn == nil {
		return nil
	}
	err := c.TCPConn.Close()
	c.TCPConn = nil
	return err
}

func (c *Connection) IsConnected() bool {
	return c.isConnected
}
