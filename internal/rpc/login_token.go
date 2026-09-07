package rpc

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

const (
	loginTokenTTL   = 30 * time.Second
	loginTokenBytes = 32
)

type loginTokenTarget struct {
	rawAuthKeyID [8]byte
	authKeyID    [8]byte
	sessionID    int64
}

type loginTokenExport struct {
	token        []byte
	expires      time.Time
	accepted     bool
	acceptedAuth domain.Authorization
}

type loginTokenAcceptStart struct {
	target loginTokenTarget
	authz  domain.Authorization
}

type loginTokenRegistry struct {
	shared store.LoginTokenRegistryStore
}

func newLoginTokenRegistry(shared ...store.LoginTokenRegistryStore) *loginTokenRegistry {
	var sharedStore store.LoginTokenRegistryStore
	if len(shared) > 0 {
		sharedStore = shared[0]
	}
	return &loginTokenRegistry{shared: sharedStore}
}

func (r *loginTokenRegistry) export(now time.Time, target loginTokenTarget, authz domain.Authorization, exceptIDs []int64) (loginTokenExport, error) {
	return r.exportContext(context.Background(), now, target, authz, exceptIDs)
}

func (r *loginTokenRegistry) exportContext(ctx context.Context, now time.Time, target loginTokenTarget, authz domain.Authorization, exceptIDs []int64) (loginTokenExport, error) {
	if r == nil {
		return loginTokenExport{}, fmt.Errorf("login token registry is nil")
	}
	if r.shared == nil {
		return loginTokenExport{}, fmt.Errorf("login token registry store is required")
	}
	return r.exportShared(ctx, now, target, authz, exceptIDs)
}

func (r *loginTokenRegistry) exportShared(ctx context.Context, now time.Time, target loginTokenTarget, authz domain.Authorization, exceptIDs []int64) (loginTokenExport, error) {
	sharedTarget := loginTokenStoreTarget(target)
	if rec, found, err := r.shared.GetLoginTokenByTarget(ctx, sharedTarget); err != nil {
		return loginTokenExport{}, err
	} else if found && rec.ExpiresAt().After(now) {
		return loginTokenExportFromStore(rec), nil
	} else if found {
		_ = r.shared.DeleteLoginToken(ctx, rec.Token, rec.Target)
	}
	for attempts := 0; attempts < 32; attempts++ {
		token := make([]byte, loginTokenBytes)
		if _, err := rand.Read(token); err != nil {
			return loginTokenExport{}, fmt.Errorf("generate login token: %w", err)
		}
		rec := store.LoginTokenRecord{
			Token:              token,
			Target:             sharedTarget,
			Authorization:      authz,
			ExceptIDs:          loginTokenExceptList(exceptIDs),
			CreatedAtUnixMilli: now.UnixMilli(),
			ExpiresAtUnixMilli: now.Add(loginTokenTTL).UnixMilli(),
		}
		created, err := r.shared.PutLoginToken(ctx, rec, loginTokenTTL)
		if err != nil {
			return loginTokenExport{}, err
		}
		if created {
			return loginTokenExport{token: append([]byte(nil), token...), expires: rec.ExpiresAt()}, nil
		}
		if rec, found, err := r.shared.GetLoginTokenByTarget(ctx, sharedTarget); err != nil {
			return loginTokenExport{}, err
		} else if found && rec.ExpiresAt().After(now) {
			return loginTokenExportFromStore(rec), nil
		} else if found {
			_ = r.shared.DeleteLoginToken(ctx, rec.Token, rec.Target)
		}
	}
	return loginTokenExport{}, fmt.Errorf("generate login token: collision")
}

func (r *loginTokenRegistry) lookup(now time.Time, token []byte) (loginTokenExport, error) {
	return r.lookupContext(context.Background(), now, token)
}

func (r *loginTokenRegistry) lookupContext(ctx context.Context, now time.Time, token []byte) (loginTokenExport, error) {
	if r == nil || len(token) == 0 {
		return loginTokenExport{}, authTokenInvalidErr()
	}
	if r.shared == nil {
		return loginTokenExport{}, fmt.Errorf("login token registry store is required")
	}
	rec, found, err := r.shared.GetLoginToken(ctx, token)
	if err != nil {
		return loginTokenExport{}, err
	}
	if !found {
		return loginTokenExport{}, authTokenInvalidErr()
	}
	if !rec.ExpiresAt().After(now) {
		_ = r.shared.DeleteLoginToken(ctx, token, rec.Target)
		return loginTokenExport{}, authTokenExpiredErr()
	}
	return loginTokenExportFromStore(rec), nil
}

func (r *loginTokenRegistry) beginAccept(now time.Time, token []byte, userID int64) (loginTokenAcceptStart, error) {
	return r.beginAcceptContext(context.Background(), now, token, userID)
}

func (r *loginTokenRegistry) beginAcceptContext(ctx context.Context, now time.Time, token []byte, userID int64) (loginTokenAcceptStart, error) {
	if r == nil || len(token) == 0 {
		return loginTokenAcceptStart{}, authTokenInvalidErr()
	}
	if r.shared == nil {
		return loginTokenAcceptStart{}, fmt.Errorf("login token registry store is required")
	}
	rec, status, err := r.shared.BeginLoginTokenAccept(ctx, token, userID, now)
	if err != nil {
		return loginTokenAcceptStart{}, err
	}
	switch status {
	case store.LoginTokenAcceptStarted:
		return loginTokenAcceptStart{
			target: loginTokenTargetFromStore(rec.Target),
			authz:  rec.Authorization,
		}, nil
	case store.LoginTokenAcceptExpired:
		_ = r.shared.DeleteLoginToken(ctx, token, rec.Target)
		return loginTokenAcceptStart{}, authTokenExpiredErr()
	case store.LoginTokenAcceptAlreadyAccepted:
		return loginTokenAcceptStart{}, authTokenAlreadyAcceptedErr()
	default:
		return loginTokenAcceptStart{}, authTokenInvalidErr()
	}
}

func (r *loginTokenRegistry) finishAccept(now time.Time, token []byte, userID int64, acceptedAuth domain.Authorization) {
	r.finishAcceptContext(context.Background(), now, token, userID, acceptedAuth)
}

func (r *loginTokenRegistry) finishAcceptContext(ctx context.Context, now time.Time, token []byte, userID int64, acceptedAuth domain.Authorization) {
	if r == nil {
		return
	}
	if r.shared == nil {
		return
	}
	_, _ = r.shared.FinishLoginTokenAccept(ctx, token, userID, acceptedAuth, now)
}

func (r *loginTokenRegistry) failAccept(token []byte) {
	r.failAcceptContext(context.Background(), token)
}

func (r *loginTokenRegistry) failAcceptContext(ctx context.Context, token []byte) {
	if r == nil {
		return
	}
	if r.shared == nil {
		return
	}
	_ = r.shared.FailLoginTokenAccept(ctx, token)
}

func loginTokenExceptList(ids []int64) []int64 {
	if len(ids) == 0 {
		return []int64{}
	}
	out := make([]int64, 0, len(ids))
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func loginTokenStoreTarget(target loginTokenTarget) store.LoginTokenTarget {
	return store.LoginTokenTarget{
		RawAuthKeyID: target.rawAuthKeyID,
		AuthKeyID:    target.authKeyID,
		SessionID:    target.sessionID,
	}
}

func loginTokenTargetFromStore(target store.LoginTokenTarget) loginTokenTarget {
	return loginTokenTarget{
		rawAuthKeyID: target.RawAuthKeyID,
		authKeyID:    target.AuthKeyID,
		sessionID:    target.SessionID,
	}
}

func loginTokenExportFromStore(rec store.LoginTokenRecord) loginTokenExport {
	if rec.Accepted {
		return loginTokenExport{accepted: true, acceptedAuth: rec.AcceptedAuthorization}
	}
	return loginTokenExport{
		token:   append([]byte(nil), rec.Token...),
		expires: rec.ExpiresAt(),
	}
}
