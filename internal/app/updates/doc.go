// Package updates 是更新状态机与投递：user 级 pts/qts/seq/date、离线差量（updates.getDifference）、
// 在线推送。第一阶段持久化 auth_key 维度的初始空状态，消息事件队列留第二阶段。
package updates
