package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/net/dns/dnsmessage"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "DNS UDP/TCP listen address")
	name := flag.String("name", "coreexec.test.", "DNS name to advertise")
	ipList := flag.String("a", "127.0.0.1,127.0.0.2", "comma-separated IPv4 A records")
	servicePort := flag.Int("service-port", 0, "CoreExec service port used when printing dns:// target")
	targetFile := flag.String("target-file", "", "optional file to write the dns://authority/name:port target")
	duration := flag.Duration("duration", 0, "optional lifetime before exiting; 0 waits for Ctrl+C/SIGTERM")
	flag.Parse()

	if *servicePort < 0 || *servicePort > 65535 {
		fatalf("-service-port must be in 0..65535")
	}
	ips, err := parseIPv4List(*ipList)
	if err != nil {
		fatalf("parse -a: %v", err)
	}
	dnsName := normalizeDNSName(*name)
	if _, err := dnsmessage.NewName(dnsName); err != nil {
		fatalf("invalid -name %q: %v", *name, err)
	}

	udpAddr, err := net.ResolveUDPAddr("udp", *addr)
	if err != nil {
		fatalf("resolve -addr: %v", err)
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		fatalf("listen udp: %v", err)
	}
	defer func() { _ = udpConn.Close() }()
	host, port, err := net.SplitHostPort(udpConn.LocalAddr().String())
	if err != nil {
		fatalf("split udp addr %q: %v", udpConn.LocalAddr().String(), err)
	}
	tcpLn, err := net.Listen("tcp", net.JoinHostPort(host, port))
	if err != nil {
		fatalf("listen tcp: %v", err)
	}
	defer func() { _ = tcpLn.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	go serveDNSUDP(ctx, udpConn, ips)
	go serveDNSTCP(ctx, tcpLn, ips)

	authority := net.JoinHostPort(host, port)
	target := ""
	if *servicePort > 0 {
		target = fmt.Sprintf("dns://%s/%s", authority, net.JoinHostPort(strings.TrimSuffix(dnsName, "."), fmt.Sprintf("%d", *servicePort)))
		if err := writeTargetFile(*targetFile, target); err != nil {
			fatalf("write target file: %v", err)
		}
	}
	fmt.Printf("coreexec dns authority listening: addr=%s name=%s a=%s\n", authority, dnsName, strings.Join(ipStrings(ips), ","))
	if target != "" {
		fmt.Printf("coreexec dns target: %s\n", target)
	}
	<-ctx.Done()
}

func parseIPv4List(raw string) ([]net.IP, error) {
	parts := strings.Split(raw, ",")
	ips := make([]net.IP, 0, len(parts))
	for _, part := range parts {
		text := strings.TrimSpace(part)
		if text == "" {
			continue
		}
		ip := net.ParseIP(text)
		if ip == nil || ip.To4() == nil {
			return nil, fmt.Errorf("%q is not an IPv4 address", text)
		}
		ips = append(ips, ip)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("at least one IPv4 A record is required")
	}
	return ips, nil
}

func normalizeDNSName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "coreexec.test."
	}
	if !strings.HasSuffix(name, ".") {
		name += "."
	}
	return name
}

func ipStrings(ips []net.IP) []string {
	out := make([]string, 0, len(ips))
	for _, ip := range ips {
		out = append(out, ip.String())
	}
	return out
}

func writeTargetFile(path, target string) error {
	if strings.TrimSpace(path) == "" || target == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(target+"\n"), 0o644)
}

func serveDNSUDP(ctx context.Context, conn *net.UDPConn, ips []net.IP) {
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()
	buf := make([]byte, 1500)
	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		resp, err := buildDNSResponse(buf[:n], ips)
		if err != nil {
			continue
		}
		_, _ = conn.WriteToUDP(resp, addr)
	}
}

func serveDNSTCP(ctx context.Context, ln net.Listener, ips []net.IP) {
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go handleDNSTCPConn(conn, ips)
	}
}

func handleDNSTCPConn(conn net.Conn, ips []net.IP) {
	defer func() { _ = conn.Close() }()
	for {
		var prefix [2]byte
		if _, err := io.ReadFull(conn, prefix[:]); err != nil {
			return
		}
		length := int(binary.BigEndian.Uint16(prefix[:]))
		if length == 0 {
			return
		}
		query := make([]byte, length)
		if _, err := io.ReadFull(conn, query); err != nil {
			return
		}
		resp, err := buildDNSResponse(query, ips)
		if err != nil {
			return
		}
		if len(resp) > 0xffff {
			return
		}
		var outPrefix [2]byte
		binary.BigEndian.PutUint16(outPrefix[:], uint16(len(resp)))
		if _, err := conn.Write(append(outPrefix[:], resp...)); err != nil {
			return
		}
	}
}

func buildDNSResponse(query []byte, ips []net.IP) ([]byte, error) {
	var parser dnsmessage.Parser
	header, err := parser.Start(query)
	if err != nil {
		return nil, err
	}
	questions := make([]dnsmessage.Question, 0, 1)
	for {
		question, err := parser.Question()
		if err == dnsmessage.ErrSectionDone {
			break
		}
		if err != nil {
			return nil, err
		}
		questions = append(questions, question)
	}
	builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{
		ID:                 header.ID,
		Response:           true,
		Authoritative:      true,
		RecursionAvailable: true,
		RCode:              dnsmessage.RCodeSuccess,
	})
	builder.EnableCompression()
	if err := builder.StartQuestions(); err != nil {
		return nil, err
	}
	for _, question := range questions {
		if err := builder.Question(question); err != nil {
			return nil, err
		}
	}
	if err := builder.StartAnswers(); err != nil {
		return nil, err
	}
	for _, question := range questions {
		if question.Type != dnsmessage.TypeA {
			continue
		}
		for _, ip := range ips {
			ip4 := ip.To4()
			if ip4 == nil {
				continue
			}
			var a [4]byte
			copy(a[:], ip4)
			if err := builder.AResource(dnsmessage.ResourceHeader{
				Name:  question.Name,
				Type:  dnsmessage.TypeA,
				Class: dnsmessage.ClassINET,
				TTL:   1,
			}, dnsmessage.AResource{A: a}); err != nil {
				return nil, err
			}
		}
	}
	return builder.Finish()
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
