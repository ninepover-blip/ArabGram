package mtprobes

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/gotd/log/logzap"
	"github.com/iamxvbaba/td/exchange"
	"github.com/iamxvbaba/td/session"
	"github.com/iamxvbaba/td/telegram"
	"github.com/iamxvbaba/td/telegram/dcs"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/transport"
	"go.uber.org/zap"
)

type Endpoint struct {
	Address    string
	DC         int
	APIID      int
	APIHash    string
	PublicKey  *rsa.PublicKey
	Obfuscated bool
	PFS        bool
	TempKeyTTL int
	Storage    telegram.SessionStorage
}

func NewClient(endpoint Endpoint) (*telegram.Client, error) {
	host, portText, err := net.SplitHostPort(endpoint.Address)
	if err != nil {
		return nil, fmt.Errorf("parse endpoint address: %w", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port <= 0 || port > 65535 {
		return nil, fmt.Errorf("invalid endpoint port %q", portText)
	}
	protocol := dcs.Protocol(transport.Intermediate)
	if endpoint.Obfuscated {
		protocol = transport.Abridged
	}
	var dialer net.Dialer
	dialTarget := net.JoinHostPort(host, portText)
	storage := endpoint.Storage
	if storage == nil {
		storage = &session.StorageMemory{}
	}
	return telegram.NewClient(endpoint.APIID, endpoint.APIHash, telegram.Options{
		PublicKeys: []exchange.PublicKey{{RSA: endpoint.PublicKey}},
		DC:         endpoint.DC,
		Resolver: dcs.Plain(dcs.PlainOptions{
			Protocol:   protocol,
			Obfuscated: endpoint.Obfuscated,
			Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return dialer.DialContext(ctx, network, dialTarget)
			},
		}),
		DCList: dcs.List{Options: []tg.DCOption{{
			ID: endpoint.DC, IPAddress: host, Port: port, Static: true,
		}}},
		SessionStorage: storage,
		UpdateHandler: telegram.UpdateHandlerFunc(func(context.Context, tg.UpdatesClass) error {
			return nil
		}),
		EnablePFS:  endpoint.PFS,
		TempKeyTTL: endpoint.TempKeyTTL,
		Device:     telegram.DeviceTDesktopWindows(),
		Logger:     logzap.New(zap.NewNop()),
	}), nil
}

func LoadRSAPublicKey(path string) (*rsa.PublicKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", path, err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("RSA key is not PEM")
	}
	if private, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return &private.PublicKey, nil
	}
	if parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		if private, ok := parsed.(*rsa.PrivateKey); ok {
			return &private.PublicKey, nil
		}
	}
	if public, err := x509.ParsePKCS1PublicKey(block.Bytes); err == nil {
		return public, nil
	}
	if parsed, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		if public, ok := parsed.(*rsa.PublicKey); ok {
			return public, nil
		}
	}
	return nil, errors.New("PEM does not contain an RSA private or public key")
}
