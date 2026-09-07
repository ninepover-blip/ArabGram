package sfu

import (
	"errors"
	"io"
	"net"
	"time"

	"github.com/pion/transport/v4/packetio"
)

// demuxer 把 ICE conn 上的混合流量按 RFC 7983 首字节分发：
//   - [20,63]   → DTLS（握手与应用数据）
//   - [128,191] → RTP/RTCP（再按 RTCP PT 200..207 细分）
//
// 写方向全部直写底层 ICE conn（同一 5 元组，RTCP-mux）。
type demuxer struct {
	base  net.Conn
	dtls  *packetio.Buffer
	srtp  *packetio.Buffer
	srtcp *packetio.Buffer
	done  chan struct{}
}

func newDemuxer(base net.Conn) *demuxer {
	d := &demuxer{
		base:  base,
		dtls:  packetio.NewBuffer(),
		srtp:  packetio.NewBuffer(),
		srtcp: packetio.NewBuffer(),
		done:  make(chan struct{}),
	}
	go d.readLoop()
	return d
}

func (d *demuxer) readLoop() {
	defer close(d.done)
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
			if n >= 2 && isRTCPPayloadType(buf[1]) {
				_, _ = d.srtcp.Write(buf[:n])
			} else {
				_, _ = d.srtp.Write(buf[:n])
			}
		default:
			// STUN 已被 ICE 层消费；其余丢弃。
		}
	}
}

func isRTCPPayloadType(pt byte) bool {
	// RTCP packet type 范围（SR/RR/SDES/BYE/APP/RTPFB/PSFB...）。
	return pt >= 192 && pt <= 223
}

func (d *demuxer) Close() {
	_ = d.dtls.Close()
	_ = d.srtp.Close()
	_ = d.srtcp.Close()
}

// demuxConn 把单一 buffer 包成 net.Conn：读取自 buffer，写直达底层。
type demuxConn struct {
	buf  *packetio.Buffer
	base net.Conn
}

func (c *demuxConn) Read(b []byte) (int, error) {
	n, err := c.buf.Read(b)
	if errors.Is(err, io.EOF) || errors.Is(err, packetio.ErrFull) {
		return n, err
	}
	return n, err
}

func (c *demuxConn) Write(b []byte) (int, error) { return c.base.Write(b) }
func (c *demuxConn) Close() error                { return c.buf.Close() }
func (c *demuxConn) LocalAddr() net.Addr         { return c.base.LocalAddr() }
func (c *demuxConn) RemoteAddr() net.Addr        { return c.base.RemoteAddr() }

// SetDeadline/SetReadDeadline 透传给 packetio.Buffer：DTLS 握手依赖读超时
// 推进重传与失败退出（pion/dtls v3 默认 Handshake 无限期阻塞，调用方必须
// 在握手期间设硬上限，否则停滞的握手会把 goroutine 挂死）。
func (c *demuxConn) SetDeadline(t time.Time) error     { return c.buf.SetReadDeadline(t) }
func (c *demuxConn) SetReadDeadline(t time.Time) error { return c.buf.SetReadDeadline(t) }

// 写方向直达底层 ICE conn，无队列可超时。
func (c *demuxConn) SetWriteDeadline(t time.Time) error { return nil }

func (d *demuxer) dtlsConn() net.Conn  { return &demuxConn{buf: d.dtls, base: d.base} }
func (d *demuxer) srtpConn() net.Conn  { return &demuxConn{buf: d.srtp, base: d.base} }
func (d *demuxer) srtcpConn() net.Conn { return &demuxConn{buf: d.srtcp, base: d.base} }
