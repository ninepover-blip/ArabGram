package domain

import (
	"hash/fnv"
	"net/url"
	"strings"
)

// NormalizeWebPageURL 把用户消息里的链接规范化为去重用的稳定形态：仅接受 http/https、
// 拒绝带 userinfo 的 URL（SSRF/伪装防御）、小写 scheme 与 host、去掉默认端口与 fragment，
// 保留 path 与 query 原样。返回规范化 URL 与是否可预览（不可预览返回 false）。
func NormalizeWebPageURL(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", false
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", false
	}
	if u.User != nil {
		return "", false
	}
	host := strings.ToLower(u.Host)
	if host == "" {
		return "", false
	}
	// 去掉与 scheme 对应的默认端口，避免 example.com 与 example.com:80 哈希不同。
	if h, port, ok := splitHostPort(host); ok {
		if (scheme == "http" && port == "80") || (scheme == "https" && port == "443") {
			host = h
		}
	}
	out := scheme + "://" + host + u.EscapedPath()
	if u.RawQuery != "" {
		out += "?" + u.RawQuery
	}
	return out, true
}

// splitHostPort 拆分 host:port；无端口时 ok=false。不用 net.SplitHostPort 以免 IPv6
// 字面量裸 host（无端口）被当作错误。
func splitHostPort(host string) (string, string, bool) {
	// IPv6 字面量形如 [::1]:443；只在末尾 ] 之后找端口。
	idx := strings.LastIndexByte(host, ':')
	if idx < 0 {
		return host, "", false
	}
	if close := strings.LastIndexByte(host, ']'); close >= 0 && idx < close {
		return host, "", false
	}
	return host[:idx], host[idx+1:], true
}

// IsPendingWebPageMedia 报告 media 是否为 ID==id 的 pending 链接预览占位（异步解析的
// 幂等守卫：仅这种状态才允许被解析结果就地替换）。
func IsPendingWebPageMedia(m *MessageMedia, id int64) bool {
	return m != nil &&
		m.Kind == MessageMediaKindWebPage &&
		m.WebPage != nil &&
		m.WebPage.State == MessageWebPageStatePending &&
		m.WebPage.ID == id
}

// WebPageURLHash 计算规范化 URL 的稳定 63-bit 哈希，同时用作 web_pages 行的主键与
// webPage id（保证 pending 占位与 done 解析携带同一 id）。FNV-1a/64 取正。
func WebPageURLHash(normalized string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(normalized))
	return int64(h.Sum64() & 0x7fffffffffffffff)
}
