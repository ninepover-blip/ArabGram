package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"strings"
	"time"

	"github.com/pion/dtls/v3"
	"github.com/pion/dtls/v3/pkg/crypto/selfsign"
	dtlsnet "github.com/pion/dtls/v3/pkg/net"
	"github.com/pion/ice/v4"
	"github.com/pion/rtp"
	"github.com/pion/srtp/v3"
	"github.com/pion/transport/v4/packetio"

	"telesrv/internal/sfu"
)

var errClientDTLSStateMissing = errors.New("client dtls state missing")

func main() {
	controlAddr := flag.String("control-addr", "", "SFU gRPC control address or target")
	controlToken := flag.String("control-token", "", "SFU control bearer token")
	tlsCAFile := flag.String("control-grpc-tls-ca-file", "", "Root CA bundle for SFU gRPC control")
	tlsServerName := flag.String("control-grpc-tls-server-name", "", "Server name for SFU gRPC control certificate validation")
	tlsClientCertFile := flag.String("control-grpc-tls-client-cert-file", "", "Client certificate for SFU gRPC control mTLS")
	tlsClientKeyFile := flag.String("control-grpc-tls-client-key-file", "", "Client private key for SFU gRPC control mTLS")
	callID := flag.Int64("call-id", 0, "probe call ID; generated when omitted")
	aliceUserID := flag.Int64("alice-user-id", 710001, "publisher user ID")
	bobUserID := flag.Int64("bob-user-id", 710002, "subscriber user ID")
	minForwardedPackets := flag.Int("min-forwarded-packets", 1, "minimum forwarded RTP packets Bob must receive")
	mediaDuration := flag.Duration("media-duration", 0, "minimum media forwarding duration to observe after the first forwarded packet")
	timeout := flag.Duration("timeout", 30*time.Second, "overall probe timeout")
	flag.Parse()

	if strings.TrimSpace(*controlAddr) == "" {
		fatalf("-control-addr is required")
	}
	if strings.TrimSpace(*controlToken) == "" {
		fatalf("-control-token is required")
	}
	if *timeout <= 0 {
		fatalf("-timeout must be positive")
	}
	if *aliceUserID <= 0 || *bobUserID <= 0 || *aliceUserID == *bobUserID {
		fatalf("-alice-user-id and -bob-user-id must be positive and different")
	}
	if *minForwardedPackets <= 0 {
		fatalf("-min-forwarded-packets must be positive")
	}
	if *mediaDuration < 0 {
		fatalf("-media-duration must not be negative")
	}
	if *callID <= 0 {
		*callID = 970000000000 + time.Now().UnixMilli()%1000000000
	}

	remote, closeRemote, err := newRemoteControl(remoteConfig{
		token:             *controlToken,
		tlsCAFile:         *tlsCAFile,
		tlsServerName:     *tlsServerName,
		tlsClientCertFile: *tlsClientCertFile,
		tlsClientKeyFile:  *tlsClientKeyFile,
	})
	if err != nil {
		fatalf("init remote control: %v", err)
	}
	defer func() { _ = closeRemote() }()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	result, err := runProbe(ctx, remote, probeConfig{
		controlAddr:         strings.TrimSpace(*controlAddr),
		callID:              *callID,
		aliceUserID:         *aliceUserID,
		bobUserID:           *bobUserID,
		minForwardedPackets: *minForwardedPackets,
		mediaDuration:       *mediaDuration,
	})
	if err != nil {
		fatalf("sfu media probe: %v", err)
	}
	fmt.Printf("sfu media probe ok: control_plane=grpc control=%s call_id=%d users=%d,%d packets=%d duration=%s\n",
		*controlAddr, *callID, *aliceUserID, *bobUserID, result.forwardedPackets, result.observedDuration.Truncate(time.Millisecond))
}

type remoteControl interface {
	sfu.RemoteService
}

type remoteConfig struct {
	token             string
	tlsCAFile         string
	tlsServerName     string
	tlsClientCertFile string
	tlsClientKeyFile  string
}

func newRemoteControl(cfg remoteConfig) (remoteControl, func() error, error) {
	remote, err := sfu.NewGRPCRemoteService(sfu.GRPCRemoteConfig{
		Token:         cfg.token,
		TLSCAFile:     cfg.tlsCAFile,
		TLSServerName: cfg.tlsServerName,
		TLSCertFile:   cfg.tlsClientCertFile,
		TLSKeyFile:    cfg.tlsClientKeyFile,
	})
	if err != nil {
		return nil, nil, err
	}
	return remote, remote.Close, nil
}

type probeConfig struct {
	controlAddr         string
	callID              int64
	aliceUserID         int64
	bobUserID           int64
	minForwardedPackets int
	mediaDuration       time.Duration
}

type probeResult struct {
	forwardedPackets int
	observedDuration time.Duration
}

func runProbe(ctx context.Context, remote remoteControl, cfg probeConfig) (probeResult, error) {
	owner := sfu.OwnerRecord{CallID: cfg.callID, ControlAddr: cfg.controlAddr}
	alice, err := newMediaClient(0xA11CE)
	if err != nil {
		return probeResult{}, err
	}
	defer alice.close()
	bob, err := newMediaClient(0xB0B)
	if err != nil {
		return probeResult{}, err
	}
	defer bob.close()
	defer func() { _ = remote.CloseRoom(context.Background(), owner, cfg.callID) }()

	answerA, err := remote.Join(ctx, owner, cfg.callID, cfg.aliceUserID, sfu.EndpointMain, alice.offer())
	if err != nil {
		return probeResult{}, fmt.Errorf("join alice: %w", err)
	}
	if len(answerA.Candidates) == 0 {
		return probeResult{}, fmt.Errorf("join alice returned no ICE candidates")
	}
	answerB, err := remote.Join(ctx, owner, cfg.callID, cfg.bobUserID, sfu.EndpointMain, bob.offer())
	if err != nil {
		return probeResult{}, fmt.Errorf("join bob: %w", err)
	}
	if len(answerB.Candidates) == 0 {
		return probeResult{}, fmt.Errorf("join bob returned no ICE candidates")
	}

	errCh := make(chan error, 2)
	go func() { errCh <- alice.connect(ctx, answerA) }()
	go func() { errCh <- bob.connect(ctx, answerB) }()
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			return probeResult{}, fmt.Errorf("connect media client: %w", err)
		}
	}

	stop := make(chan struct{})
	defer close(stop)
	go alice.sendOpusLoop(stop)

	stream, ssrc, err := bob.acceptStream(ctx)
	if err != nil {
		return probeResult{}, fmt.Errorf("bob accept forwarded stream: %w", err)
	}
	if ssrc != alice.ssrc {
		return probeResult{}, fmt.Errorf("forwarded ssrc = %#x, want alice %#x", ssrc, alice.ssrc)
	}
	result, err := observeForwardedRTP(ctx, stream, alice.ssrc, cfg.minForwardedPackets, cfg.mediaDuration)
	if err != nil {
		return probeResult{}, fmt.Errorf("bob observe forwarded RTP: %w", err)
	}

	if err := waitAlive(ctx, remote, owner, cfg.callID, cfg.aliceUserID); err != nil {
		return probeResult{}, err
	}
	if err := remote.Leave(ctx, owner, cfg.callID, cfg.aliceUserID, sfu.EndpointMain); err != nil {
		return probeResult{}, fmt.Errorf("leave alice: %w", err)
	}
	if err := remote.Leave(ctx, owner, cfg.callID, cfg.bobUserID, sfu.EndpointMain); err != nil {
		return probeResult{}, fmt.Errorf("leave bob: %w", err)
	}
	if err := remote.CloseRoom(ctx, owner, cfg.callID); err != nil {
		return probeResult{}, fmt.Errorf("close room: %w", err)
	}
	return result, nil
}

type mediaClient struct {
	ufrag       string
	pwd         string
	cert        tls.Certificate
	fingerprint string
	ssrc        uint32

	session *srtp.SessionSRTP
	write   *srtp.WriteStreamSRTP
	closeFn []func()
}

func newMediaClient(ssrc uint32) (*mediaClient, error) {
	cert, err := selfsign.GenerateSelfSigned()
	if err != nil {
		return nil, fmt.Errorf("client cert: %w", err)
	}
	fp, err := certificateFingerprint(cert.Certificate[0])
	if err != nil {
		return nil, err
	}
	ufrag, err := randomICEString(8)
	if err != nil {
		return nil, err
	}
	pwd, err := randomICEString(24)
	if err != nil {
		return nil, err
	}
	return &mediaClient{ufrag: ufrag, pwd: pwd, cert: cert, fingerprint: fp, ssrc: ssrc}, nil
}

func (c *mediaClient) offer() sfu.ClientOffer {
	return sfu.ClientOffer{
		AudioSSRC:         c.ssrc,
		Ufrag:             c.ufrag,
		Pwd:               c.pwd,
		FingerprintSHA256: c.fingerprint,
	}
}

func (c *mediaClient) connect(ctx context.Context, answer sfu.ServerAnswer) error {
	agent, err := ice.NewAgent(&ice.AgentConfig{
		NetworkTypes:    []ice.NetworkType{ice.NetworkTypeUDP4},
		CandidateTypes:  []ice.CandidateType{ice.CandidateTypeHost},
		LocalUfrag:      c.ufrag,
		LocalPwd:        c.pwd,
		IncludeLoopback: true,
	})
	if err != nil {
		return err
	}
	c.closeFn = append(c.closeFn, func() { _ = agent.Close() })
	if err := agent.OnCandidate(func(ice.Candidate) {}); err != nil {
		return err
	}
	if err := agent.GatherCandidates(); err != nil {
		return err
	}
	for _, cand := range answer.Candidates {
		remote, err := ice.NewCandidateHost(&ice.CandidateHostConfig{
			Network:   cand.Protocol,
			Address:   cand.IP,
			Port:      cand.Port,
			Component: 1,
		})
		if err != nil {
			return err
		}
		if err := agent.AddRemoteCandidate(remote); err != nil {
			return err
		}
	}
	conn, err := agent.Accept(ctx, answer.Ufrag, answer.Pwd)
	if err != nil {
		return err
	}
	demux := newProbeDemuxer(conn)
	c.closeFn = append(c.closeFn, demux.Close)
	dtlsRaw := demux.dtlsConn()
	dtlsConn, err := dtls.Server(dtlsnet.PacketConnFromConn(dtlsRaw), dtlsRaw.RemoteAddr(), &dtls.Config{
		Certificates:         []tls.Certificate{c.cert},
		ClientAuth:           dtls.RequireAnyClientCert,
		InsecureSkipVerify:   true,
		ExtendedMasterSecret: dtls.RequireExtendedMasterSecret,
		SRTPProtectionProfiles: []dtls.SRTPProtectionProfile{
			dtls.SRTP_AEAD_AES_128_GCM,
			dtls.SRTP_AES128_CM_HMAC_SHA1_80,
		},
	})
	if err != nil {
		return err
	}
	hsCtx, hsCancel := context.WithTimeout(ctx, 20*time.Second)
	err = dtlsConn.HandshakeContext(hsCtx)
	hsCancel()
	if err != nil {
		return err
	}
	state, ok := dtlsConn.ConnectionState()
	if !ok || len(state.PeerCertificates) == 0 {
		return errClientDTLSStateMissing
	}
	gotFP, err := certificateFingerprint(state.PeerCertificates[0])
	if err != nil || normalizeFingerprint(gotFP) != normalizeFingerprint(answer.FingerprintSHA256) {
		return fmt.Errorf("sfu fingerprint mismatch: %v / %s vs %s", err, gotFP, answer.FingerprintSHA256)
	}
	profile, _ := dtlsConn.SelectedSRTPProtectionProfile()
	srtpConfig := &srtp.Config{}
	switch profile {
	case dtls.SRTP_AEAD_AES_128_GCM:
		srtpConfig.Profile = srtp.ProtectionProfileAeadAes128Gcm
	case dtls.SRTP_AES128_CM_HMAC_SHA1_80:
		srtpConfig.Profile = srtp.ProtectionProfileAes128CmHmacSha1_80
	default:
		return fmt.Errorf("unexpected srtp profile %v", profile)
	}
	if err := srtpConfig.ExtractSessionKeysFromDTLS(&state, false); err != nil {
		return err
	}
	c.session, err = srtp.NewSessionSRTP(demux.srtpConn(), srtpConfig)
	if err != nil {
		return err
	}
	c.write, err = c.session.OpenWriteStream()
	return err
}

func (c *mediaClient) sendOpusLoop(stop <-chan struct{}) {
	seq := uint16(1)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			_ = c.sendOpusPacket(seq, []byte{0xDE, 0xAD, byte(seq)}, 0x7F)
			if seq == math.MaxUint16 {
				seq = 1
			} else {
				seq++
			}
		}
	}
}

func (c *mediaClient) sendOpusPacket(seq uint16, payload []byte, audioLevel byte) error {
	if c.write == nil {
		return fmt.Errorf("client not connected")
	}
	pkt := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    111,
			SequenceNumber: seq,
			Timestamp:      uint32(seq) * 960,
			SSRC:           c.ssrc,
		},
		Payload: payload,
	}
	pkt.Header.Extension = true
	pkt.Header.ExtensionProfile = 0xBEDE
	if err := pkt.Header.SetExtension(1, []byte{audioLevel}); err != nil {
		return err
	}
	raw, err := pkt.Marshal()
	if err != nil {
		return err
	}
	_, err = c.write.Write(raw)
	return err
}

func (c *mediaClient) acceptStream(ctx context.Context) (*srtp.ReadStreamSRTP, uint32, error) {
	if c.session == nil {
		return nil, 0, fmt.Errorf("client not connected")
	}
	type accepted struct {
		stream *srtp.ReadStreamSRTP
		ssrc   uint32
		err    error
	}
	got := make(chan accepted, 1)
	go func() {
		stream, ssrc, err := c.session.AcceptStream()
		got <- accepted{stream: stream, ssrc: ssrc, err: err}
	}()
	select {
	case in := <-got:
		return in.stream, in.ssrc, in.err
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
}

func (c *mediaClient) close() {
	for i := len(c.closeFn) - 1; i >= 0; i-- {
		c.closeFn[i]()
	}
}

func readRTPPacket(ctx context.Context, stream *srtp.ReadStreamSRTP) (rtp.Packet, error) {
	type result struct {
		packet rtp.Packet
		err    error
	}
	got := make(chan result, 1)
	go func() {
		buf := make([]byte, 1500)
		n, err := stream.Read(buf)
		if err != nil {
			got <- result{err: err}
			return
		}
		var pkt rtp.Packet
		if err := pkt.Unmarshal(buf[:n]); err != nil {
			got <- result{err: err}
			return
		}
		got <- result{packet: pkt}
	}()
	select {
	case res := <-got:
		return res.packet, res.err
	case <-ctx.Done():
		return rtp.Packet{}, ctx.Err()
	}
}

func observeForwardedRTP(ctx context.Context, stream *srtp.ReadStreamSRTP, wantSSRC uint32, minPackets int, minDuration time.Duration) (probeResult, error) {
	start := time.Time{}
	deadline := time.Time{}
	forwarded := 0
	for {
		packet, err := readRTPPacket(ctx, stream)
		if err != nil {
			return probeResult{}, err
		}
		if packet.PayloadType != 111 || packet.SSRC != wantSSRC {
			return probeResult{}, fmt.Errorf("forwarded packet header = %+v", packet.Header)
		}
		if ext := packet.Header.GetExtension(1); len(ext) != 1 || ext[0] != 0x7F {
			return probeResult{}, fmt.Errorf("audio-level RTP extension lost: %v", ext)
		}
		forwarded++
		now := time.Now()
		if start.IsZero() {
			start = now
			if minDuration > 0 {
				deadline = start.Add(minDuration)
			}
		}
		observed := now.Sub(start)
		if forwarded >= minPackets && (minDuration == 0 || !now.Before(deadline)) {
			return probeResult{forwardedPackets: forwarded, observedDuration: observed}, nil
		}
	}
}

func waitAlive(ctx context.Context, remote remoteControl, owner sfu.OwnerRecord, callID, userID int64) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		alive, err := remote.AliveUserIDs(ctx, owner, callID)
		if err == nil {
			for _, id := range alive {
				if id == userID {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			if err != nil {
				return fmt.Errorf("wait alive user %d: %w", userID, err)
			}
			return fmt.Errorf("wait alive user %d: %w", userID, ctx.Err())
		case <-ticker.C:
		}
	}
}

type probeDemuxer struct {
	base  net.Conn
	dtls  *packetio.Buffer
	srtp  *packetio.Buffer
	srtcp *packetio.Buffer
}

func newProbeDemuxer(base net.Conn) *probeDemuxer {
	d := &probeDemuxer{
		base:  base,
		dtls:  packetio.NewBuffer(),
		srtp:  packetio.NewBuffer(),
		srtcp: packetio.NewBuffer(),
	}
	go d.readLoop()
	return d
}

func (d *probeDemuxer) readLoop() {
	buf := make([]byte, 1500)
	for {
		n, err := d.base.Read(buf)
		if err != nil {
			_ = d.dtls.Close()
			_ = d.srtp.Close()
			_ = d.srtcp.Close()
			return
		}
		if n == 0 {
			continue
		}
		first := buf[0]
		switch {
		case first >= 20 && first <= 63:
			_, _ = d.dtls.Write(buf[:n])
		case first >= 128 && first <= 191:
			if n >= 2 && buf[1] >= 192 && buf[1] <= 223 {
				_, _ = d.srtcp.Write(buf[:n])
			} else {
				_, _ = d.srtp.Write(buf[:n])
			}
		default:
		}
	}
}

func (d *probeDemuxer) Close() {
	_ = d.dtls.Close()
	_ = d.srtp.Close()
	_ = d.srtcp.Close()
}

func (d *probeDemuxer) dtlsConn() net.Conn  { return &probeDemuxConn{buf: d.dtls, base: d.base} }
func (d *probeDemuxer) srtpConn() net.Conn  { return &probeDemuxConn{buf: d.srtp, base: d.base} }
func (d *probeDemuxer) srtcpConn() net.Conn { return &probeDemuxConn{buf: d.srtcp, base: d.base} }

type probeDemuxConn struct {
	buf  *packetio.Buffer
	base net.Conn
}

func (c *probeDemuxConn) Read(b []byte) (int, error) {
	n, err := c.buf.Read(b)
	if errors.Is(err, io.EOF) || errors.Is(err, packetio.ErrFull) {
		return n, err
	}
	return n, err
}
func (c *probeDemuxConn) Write(b []byte) (int, error)   { return c.base.Write(b) }
func (c *probeDemuxConn) Close() error                  { return c.buf.Close() }
func (c *probeDemuxConn) LocalAddr() net.Addr           { return c.base.LocalAddr() }
func (c *probeDemuxConn) RemoteAddr() net.Addr          { return c.base.RemoteAddr() }
func (c *probeDemuxConn) SetDeadline(t time.Time) error { return c.buf.SetReadDeadline(t) }
func (c *probeDemuxConn) SetReadDeadline(t time.Time) error {
	return c.buf.SetReadDeadline(t)
}
func (c *probeDemuxConn) SetWriteDeadline(time.Time) error { return nil }

const iceAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomICEString(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("random ice string: %w", err)
	}
	var b strings.Builder
	for _, v := range buf {
		b.WriteByte(iceAlphabet[int(v)%len(iceAlphabet)])
	}
	return b.String(), nil
}

func certificateFingerprint(der []byte) (string, error) {
	if _, err := x509.ParseCertificate(der); err != nil {
		return "", fmt.Errorf("parse cert: %w", err)
	}
	sum := sha256.Sum256(der)
	parts := make([]string, len(sum))
	for i, b := range sum {
		parts[i] = strings.ToUpper(hex.EncodeToString([]byte{b}))
	}
	return strings.Join(parts, ":"), nil
}

func normalizeFingerprint(fp string) string {
	return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(fp), ":", ""))
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
