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
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"go.uber.org/zap"
)

var _ net.Listener = (*reconnectingTCP)(nil)

func newTCPNetworkAddress(network, address string) (caddy.NetworkAddress, error) {
	realNet, ok := strings.CutPrefix(network, "reconnect+")
	if !ok || !(realNet == "tcp" || realNet == "tcp4" || realNet == "tcp6") {
		return caddy.NetworkAddress{}, fmt.Errorf("network not supported: %s", network)
	}
	return newNetworkAddress(realNet, address)
}

type reconnectingTCP struct {
	ctx    context.Context
	cancel context.CancelFunc

	config net.ListenConfig
	na     caddy.NetworkAddress

	connectOnce       sync.Once
	stoppedConnecting chan struct{}
	connected         chan struct{}
	listener          net.Listener
}

func (l *reconnectingTCP) connect() {
	l.connectOnce.Do(func() {
		defer close(l.stoppedConnecting)

		t := time.NewTicker(time.Second)
		for i := int64(0); ; i++ {
			ctx, cancel := context.WithTimeout(l.ctx, 5*time.Second)
			listener, err := l.na.Listen(ctx, 0, l.config)
			cancel()

			// Return if listener could be opened.
			if ln, ok := listener.(net.Listener); ok && err == nil {
				l.setListener(ln)
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
					log("Unable to open listener for 60 attempts. "+
						"Start logging at debug level.",
						zap.String("network", l.na.Network),
						zap.String("address", l.na.JoinHostPort(0)),
					)
				}
				// After 60 attempts, demote to debug level.
				if i > 60 {
					log = logger.Debug
				}
				log("Unable to open listener. Reconnecting...",
					zap.String("network", l.na.Network),
					zap.String("address", l.na.JoinHostPort(0)),
					zap.Int64("attempt", i),
					zap.Error(err),
				)
			}
		}
	})
}

func (l *reconnectingTCP) Addr() net.Addr {
	select {
	case <-l.connected:
		return l.listener.Addr()
	default:
		addr, err := net.ResolveTCPAddr(l.na.Network, l.na.JoinHostPort(0))
		if err != nil {
			return &net.TCPAddr{Port: int(l.na.StartPort)}
		}
		return addr
	}
}

func (l *reconnectingTCP) Accept() (net.Conn, error) {
	l.connect()
	select {
	case <-l.connected:
		return l.listener.Accept()
	case <-l.ctx.Done():
		return nil, &net.OpError{
			Op:   "accept",
			Net:  l.Addr().Network(),
			Addr: l.Addr(),
			Err:  errClosed,
		}
	}
}

func (l *reconnectingTCP) setListener(listener net.Listener) {
	logger.Info("Opened listener",
		zap.String("network", l.na.Network),
		zap.String("address", l.na.JoinHostPort(0)),
	)
	l.listener = listener
	close(l.connected)
}

// Close closes the listener. It cancels the context such that the reconnecting
// loop terminates.
func (l *reconnectingTCP) Close() error {
	l.cancel()
	<-l.stoppedConnecting

	if l.listener != nil {
		return l.listener.Close()
	}
	return nil
}
