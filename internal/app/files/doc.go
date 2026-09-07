// Package files 是文件应用服务：upload 分片累积、blob 落盘、getFile 下载，
// 以及把上传文件组装成 Photo / Document（头像、图片/文件/贴纸消息）。
//
// 类型边界：本包只用 domain / store 类型，不依赖 tg.*；
// rpc 层负责 tg.InputFileLocation / InputMedia ↔ domain 的转换。
package files
