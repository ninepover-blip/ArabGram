// Package memory 提供 store 各接口的内存实现，用作测试替身与本地开发兜底。
//
// 与 store/postgres、store/redisstore 对称：store 主包只定义接口与 DTO，
// 三种后端实现各自独立成包。
package memory
