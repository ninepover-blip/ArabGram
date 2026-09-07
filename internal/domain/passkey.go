package domain

import "errors"

// Passkey 相关错误。
var (
	ErrPasskeyChallengeInvalid  = errors.New("passkey challenge invalid")
	ErrPasskeyNotFound          = errors.New("passkey not found")
	ErrPasskeyInvalid           = errors.New("passkey invalid")
	ErrPasskeyUserHandleInvalid = errors.New("passkey user handle invalid")
)

// PasskeyChallengePurpose 区分注册挑战与登录挑战。
type PasskeyChallengePurpose string

const (
	PasskeyChallengeLogin    PasskeyChallengePurpose = "login"
	PasskeyChallengeRegister PasskeyChallengePurpose = "register"
)

// PasskeyChallenge 是一次性 WebAuthn 挑战的元数据(短 TTL、用后即焚)。
// 注册挑战绑定发起用户;登录挑战(discoverable)UserID=0,用户由 user_handle 反查。
type PasskeyChallenge struct {
	Purpose   PasskeyChallengePurpose
	UserID    int64
	ExpiresAt int64 // unix 秒
}

// PasskeyCredential 是一条已注册的 passkey 凭据(WebAuthn 公钥)。
type PasskeyCredential struct {
	CredentialID []byte // 原始 credential id 字节(对外以 base64url 暴露)
	UserID       int64
	PublicKey    []byte // COSE 公钥原始字节
	SignCount    uint32
	AAGUID       []byte
	Name         string
	Transports   []string
	CreatedAt    int64 // unix 秒
	LastUsedAt   int64 // unix 秒;0 表示从未用于登录
}

// Clone 深拷贝(切片字段),避免内存 store 暴露内部引用。
func (c PasskeyCredential) Clone() PasskeyCredential {
	out := c
	out.CredentialID = append([]byte(nil), c.CredentialID...)
	out.PublicKey = append([]byte(nil), c.PublicKey...)
	out.AAGUID = append([]byte(nil), c.AAGUID...)
	out.Transports = append([]string(nil), c.Transports...)
	return out
}
