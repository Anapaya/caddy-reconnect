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

// Package reconnect implements a custom network type that allows binding to
// addresses that are not available at startup time.
package reconnect

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

var (
	_ caddy.Module = (*reconnect)(nil)

	_ caddy.ListenerFunc = reconnect{}.listenTCP
	_ caddy.ListenerFunc = reconnect{}.listenUDP
)

var (
	logger    = caddy.Log().Named("reconnect")
	errClosed = fmt.Errorf("listener 'closed'")
)

func init() {
	caddy.RegisterModule(reconnect{})
	caddy.RegisterNetwork("reconnect+tcp", reconnect{}.listenTCP)
	caddy.RegisterNetwork("reconnect+tcp4", reconnect{}.listenTCP)
	caddy.RegisterNetwork("reconnect+tcp6", reconnect{}.listenTCP)
	caddy.RegisterNetwork("reconnect+udp", reconnect{}.listenUDP)
	caddy.RegisterNetwork("reconnect+udp4", reconnect{}.listenUDP)
	caddy.RegisterNetwork("reconnect+udp6", reconnect{}.listenUDP)
	caddyhttp.RegisterNetworkHTTP3("reconnect+tcp", "reconnect+udp")
	caddyhttp.RegisterNetworkHTTP3("reconnect+tcp4", "reconnect+udp4")
	caddyhttp.RegisterNetworkHTTP3("reconnect+tcp6", "reconnect+udp6")
}

// reconnect is a module that provides an additional "reconnect" network type
// that can be used to reconnect to a [network address] if the initial
// connection fails. Caddy will bind to the address as soon as it is available.
// Until that point, the listener will block in the Accept() loop. This is
// useful if you want to configure Caddy to bind on an address that is
// potentially not available at startup time.
//
// You can configure the following networks:
// - reconnect+tcp
// - reconnect+tcp4
// - reconnect+tcp6
// - reconnect+udp
// - reconnect+udp4
// - reconnect+udp6
//
// These are equivalent to the standard networks, except that they will block
// until the address is available.
//
// For example, to start Caddy as an http server on 192.168.1.2:443, even if
// that address is not available at startup time, you can add the following
// listener to the [apps.http.servers.{srv}.listen] list:
//
//	"listen": ["reconnect+tcp/192.168.1.2:443"]
//
// Note: This module has only been tested with Linux. Other operating systems
// might not work as intended.
//
// [apps.http.servers.{srv}.listen]: https://caddyserver.com/docs/json/apps/http/servers/listen/
// [network address]: https://caddyserver.com/docs/conventions#network-addresses
type reconnect struct{}

func (reconnect) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "reconnect",
		New: func() caddy.Module {
			return reconnect{}
		},
	}
}

func (reconnect) listenTCP(
	ctx context.Context,
	network string,
	address string,
	cfg net.ListenConfig,
) (any, error) {
	realNet, err := underlyingNetwork(network, "tcp")
	if err != nil {
		return nil, err
	}

	na, err := newNetworkAddress(realNet, address)
	if err != nil {
		return nil, err
	}

	ln, err := na.Listen(ctx, 0, cfg)
	if err == nil {
		return ln, nil
	}

	runCtx, cancel := context.WithCancel(context.Background())
	return &reconnectingTCP{
		ctx:               runCtx,
		cancel:            cancel,
		config:            cfg,
		na:                na,
		stoppedConnecting: make(chan struct{}),
		connected:         make(chan struct{}),
	}, nil
}

func (reconnect) listenUDP(
	ctx context.Context,
	network string,
	address string,
	cfg net.ListenConfig,
) (any, error) {
	realNet, err := underlyingNetwork(network, "udp")
	if err != nil {
		return nil, err
	}

	na, err := newNetworkAddress(realNet, address)
	if err != nil {
		return nil, err
	}

	conn, err := na.Listen(ctx, 0, cfg)
	if err == nil {
		return conn, nil
	}

	runCtx, cancel := context.WithCancel(context.Background())
	return &reconnectingUDP{
		ctx:               runCtx,
		cancel:            cancel,
		config:            cfg,
		na:                na,
		stoppedConnecting: make(chan struct{}),
		connected:         make(chan struct{}),
	}, nil
}

func underlyingNetwork(network, protocol string) (string, error) {
	realNet, ok := strings.CutPrefix(network, "reconnect+")
	if !ok || !(realNet == protocol || realNet == protocol+"4" || realNet == protocol+"6") {
		return "", fmt.Errorf("network not supported: %s", network)
	}
	return realNet, nil
}

func newNetworkAddress(network, address string) (caddy.NetworkAddress, error) {
	host, rawPort, err := net.SplitHostPort(address)
	if err != nil {
		return caddy.NetworkAddress{}, err
	}
	port, err := strconv.ParseUint(rawPort, 10, 32)
	if err != nil {
		return caddy.NetworkAddress{}, fmt.Errorf("invalid port: %w", err)
	}
	return caddy.NetworkAddress{
		Network:   network,
		Host:      host,
		StartPort: uint(port),
		EndPort:   uint(port),
	}, nil
}
