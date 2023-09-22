// Copyright 2023 Anapaya Systems
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package reconnect

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"go.uber.org/zap"
)

var _ net.PacketConn = (*reconnectingUDP)(nil)

type reconnectingUDP struct {
	ctx    context.Context
	cancel context.CancelFunc

	config net.ListenConfig
	na     caddy.NetworkAddress

	connectOnce       sync.Once
	stoppedConnecting chan struct{}
	connected         chan struct{}
	connection        net.PacketConn
}

func (l *reconnectingUDP) connect() {
	l.connectOnce.Do(func() {
		defer close(l.stoppedConnecting)

		t := time.NewTicker(time.Second)
		for i := int64(0); ; i++ {
			ctx, cancel := context.WithTimeout(l.ctx, 5*time.Second)
			connection, err := l.na.Listen(ctx, 0, l.config)
			cancel()

			// Return if connection could be opened.
			if c, ok := connection.(net.PacketConn); ok && err == nil {
				l.setConnection(c)
				return
			}

			select {
			case <-l.ctx.Done():
				return
			case <-t.C:
				// Only log every 10 attempts.
				if i%10 != 0 {
					continue
				}

				log := logger.Info
				if i == 60 {
					log("Unable to open connection for 60 attempts. "+
						"Start logging at debug level.",
						zap.String("network", l.na.Network),
						zap.String("address", l.na.JoinHostPort(0)),
					)
				}
				// After 60 attempts, demote to debug level.
				if i > 60 {
					log = logger.Debug
				}
				// Only log every 10 seconds.
				if i%10 == 0 {
					log("Unable to open connection. Reconnecting...",
						zap.String("network", l.na.Network),
						zap.String("address", l.na.JoinHostPort(0)),
						zap.Int64("attempt", i),
						zap.Error(err),
					)
				}
			}
		}
	})
}

func (l *reconnectingUDP) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	l.connect()
	select {
	case <-l.connected:
		return l.connection.ReadFrom(p)
	case <-l.ctx.Done():
		return 0, nil, &net.OpError{
			Op:   "read",
			Net:  l.LocalAddr().Network(),
			Addr: l.LocalAddr(),
			Err:  errClosed,
		}
	}
}

func (l *reconnectingUDP) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	l.connect()
	select {
	case <-l.connected:
		return l.connection.WriteTo(p, addr)
	case <-l.ctx.Done():
		return 0, &net.OpError{
			Op:   "write",
			Net:  l.LocalAddr().Network(),
			Addr: l.LocalAddr(),
			Err:  errClosed,
		}
	}
}

func (l *reconnectingUDP) LocalAddr() net.Addr {
	select {
	case <-l.connected:
		return l.connection.LocalAddr()
	default:
		addr, err := net.ResolveUDPAddr(l.na.Network, l.na.JoinHostPort(0))
		if err != nil {
			return &net.UDPAddr{Port: int(l.na.StartPort)}
		}
		return addr
	}
}

func (l *reconnectingUDP) setConnection(connection net.PacketConn) {
	logger.Info("Opened connection",
		zap.String("network", l.na.Network),
		zap.String("address", l.na.JoinHostPort(0)),
	)
	l.connection = connection
	close(l.connected)
}

// Close closes the connection. It cancels the context such that the reconnecting
// loop terminates.
func (l *reconnectingUDP) Close() error {
	l.cancel()
	<-l.stoppedConnecting

	if l.connection != nil {
		return l.connection.Close()
	}
	return nil
}

func (l *reconnectingUDP) SetDeadline(t time.Time) error {
	select {
	case <-l.connected:
		return l.connection.SetDeadline(t)
	default:
		logger.Info("Ignored setting deadline on uninitialized connection",
			zap.String("network", l.na.Network),
			zap.String("address", l.na.JoinHostPort(0)),
		)
		return nil
	}
}

func (l *reconnectingUDP) SetReadDeadline(t time.Time) error {
	select {
	case <-l.connected:
		return l.connection.SetReadDeadline(t)
	default:
		logger.Info("Ignored setting read deadline on uninitialized connection",
			zap.String("network", l.na.Network),
			zap.String("address", l.na.JoinHostPort(0)),
		)
		return nil
	}
}

func (l *reconnectingUDP) SetWriteDeadline(t time.Time) error {
	select {
	case <-l.connected:
		return l.connection.SetWriteDeadline(t)
	default:
		logger.Info("Ignored setting write deadline on uninitialized connection",
			zap.String("network", l.na.Network),
			zap.String("address", l.na.JoinHostPort(0)),
		)
		return nil
	}
}
