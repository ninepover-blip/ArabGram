// Package secretchat 实现私聊端对端加密（Secret Chat / EncryptedChat）的握手
// 状态机。服务端是盲中继：g_a/g_b/key_fingerprint/加密 bytes 全部不透明存储与
// 原样转发，唯一参与密码学的点是对 g_a/g_b 做 DH 范围边界校验（防弱 DH/MITM）。
// 共享密钥与明文是 E2E 客户端职责，服务端不知道也无法计算。
// 设计见 docs/secret-chat-module.md。
package secretchat

import (
	"errors"
	"math/big"

	appphone "telesrv/internal/app/phone"
)

// dhPubSize 是 g_a/g_b 的规范字节长度（2048-bit）。
const dhPubSize = 256

// ErrGAInvalid：g_a/g_b 不在合法 DH 区间 → rpc 层映射为 DH_G_A_INVALID。
var ErrGAInvalid = errors.New("secretchat: dh parameter invalid")

var (
	// dhPrimeMinusOne/边界值复用 phone 域官方 2048-bit safe prime（与 getDhConfig 下发的 p 同源）。
	dhPrimeMinusOne = new(big.Int).Sub(new(big.Int).SetBytes(appphone.DHPrime()), big.NewInt(1))
	// 2^(2048-64) 下界与 p-2^(2048-64) 上界（官方 isGoodPrime 同款边界）。
	dhLowerBound = new(big.Int).Lsh(big.NewInt(1), 2048-64)
	dhUpperBound = new(big.Int).Sub(new(big.Int).SetBytes(appphone.DHPrime()), new(big.Int).Lsh(big.NewInt(1), 2048-64))
	dhOne        = big.NewInt(1)
)

// validateDHParam 对 g_a/g_b 做范围校验，通过后左补零到 256 字节返回（规范线格式；
// 首字节为 0 被裁的合法 g_a 不能误拒）。校验用的是 big.Int 值，与补零无关。
func validateDHParam(g []byte) ([]byte, error) {
	if len(g) == 0 || len(g) > dhPubSize {
		return nil, ErrGAInvalid
	}
	x := new(big.Int).SetBytes(g)
	// 1 < x < p-1 且 2^(2048-64) < x < p-2^(2048-64)
	if x.Cmp(dhOne) <= 0 || x.Cmp(dhPrimeMinusOne) >= 0 {
		return nil, ErrGAInvalid
	}
	if x.Cmp(dhLowerBound) <= 0 || x.Cmp(dhUpperBound) >= 0 {
		return nil, ErrGAInvalid
	}
	return leftPad256(g), nil
}

// leftPad256 左补零到 256 字节（输入已保证 ≤256）。
func leftPad256(b []byte) []byte {
	if len(b) == dhPubSize {
		return append([]byte(nil), b...)
	}
	out := make([]byte, dhPubSize)
	copy(out[dhPubSize-len(b):], b)
	return out
}
