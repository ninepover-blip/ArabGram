// Package mtprotoedge 是 MTProto 连接层：TCP/WS listener、密钥交换、auth key 查找与持久化、
// 消息加解密、session/server-salt/msg-id/ack/container/gzip，以及 invokeWithLayer、initConnection、
// invokeWithoutUpdates 等 wrapper 的 unwrap。
//
// 它只把 MTProto 世界转换成「已解密、已识别 session 的 RPC 请求」，不得包含业务逻辑。
package mtprotoedge
