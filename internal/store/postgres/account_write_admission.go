package postgres

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const accountWriteAdmissionMailboxSize = 65536

var processAccountWriteAdmission = newAccountWriteAdmissionActor(accountWriteAdmissionMailboxSize)

type accountWriteAdmissionActor struct {
	commands  chan accountWriteAdmissionCommand
	nextID    atomic.Uint64
	waits     atomic.Uint64
	waitNanos atomic.Uint64
}

type accountWriteAdmissionCommandKind uint8

const (
	accountWriteAdmissionAcquire accountWriteAdmissionCommandKind = iota + 1
	accountWriteAdmissionRelease
	accountWriteAdmissionCancel
)

type accountWriteAdmissionCommand struct {
	kind    accountWriteAdmissionCommandKind
	request *accountWriteAdmissionRequest
	id      uint64
	ack     chan struct{}
}

type accountWriteAdmissionRequest struct {
	id       uint64
	userIDs  []int64
	granted  chan struct{}
	enqueued time.Time
}

type accountWriteAdmissionSnapshot struct {
	WaitCount    uint64
	WaitDuration time.Duration
}

func newAccountWriteAdmissionActor(mailboxSize int) *accountWriteAdmissionActor {
	if mailboxSize <= 0 {
		mailboxSize = 1
	}
	a := &accountWriteAdmissionActor{
		commands: make(chan accountWriteAdmissionCommand, mailboxSize),
	}
	go a.run()
	return a
}

// Acquire grants one indivisible lease for the complete account set. Waiting
// happens before BEGIN, so a contended local send never occupies a PostgreSQL
// connection. The database transaction fence remains authoritative across Core
// processes and for mutations that are not admitted by this actor.
func (a *accountWriteAdmissionActor) Acquire(ctx context.Context, userIDs ...int64) (func(), error) {
	normalized := normalizeAccountWriteUserIDs(userIDs)
	if len(normalized) == 0 {
		return func() {}, nil
	}
	req := &accountWriteAdmissionRequest{
		id:       a.nextID.Add(1),
		userIDs:  normalized,
		granted:  make(chan struct{}, 1),
		enqueued: time.Now(),
	}
	select {
	case a.commands <- accountWriteAdmissionCommand{kind: accountWriteAdmissionAcquire, request: req}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-req.granted:
		a.recordWait(req.enqueued)
		return a.releaseFunc(req.id), nil
	case <-ctx.Done():
		ack := make(chan struct{}, 1)
		// The acquire command is already owned by the actor. Cancellation must be
		// acknowledged even when the caller deadline has expired, otherwise a
		// grant/cancel race could strand an account lease permanently.
		a.commands <- accountWriteAdmissionCommand{kind: accountWriteAdmissionCancel, id: req.id, ack: ack}
		<-ack
		a.recordWait(req.enqueued)
		return nil, ctx.Err()
	}
}

func (a *accountWriteAdmissionActor) Snapshot() accountWriteAdmissionSnapshot {
	return accountWriteAdmissionSnapshot{
		WaitCount:    a.waits.Load(),
		WaitDuration: time.Duration(a.waitNanos.Load()),
	}
}

func (a *accountWriteAdmissionActor) recordWait(started time.Time) {
	a.waits.Add(1)
	a.waitNanos.Add(uint64(time.Since(started)))
}

func (a *accountWriteAdmissionActor) releaseFunc(id uint64) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			a.commands <- accountWriteAdmissionCommand{kind: accountWriteAdmissionRelease, id: id}
		})
	}
}

func (a *accountWriteAdmissionActor) run() {
	busy := make(map[int64]uint64)
	active := make(map[uint64]*accountWriteAdmissionRequest)
	pending := make([]*accountWriteAdmissionRequest, 0, 256)

	grantReady := func() {
		if len(pending) == 0 {
			return
		}
		previousLength := len(pending)
		blockedByOlder := make(map[int64]struct{})
		remaining := pending[:0]
		for _, req := range pending {
			blocked := false
			for _, userID := range req.userIDs {
				if _, held := busy[userID]; held {
					blocked = true
					break
				}
				if _, queued := blockedByOlder[userID]; queued {
					blocked = true
					break
				}
			}
			if blocked {
				remaining = append(remaining, req)
				for _, userID := range req.userIDs {
					blockedByOlder[userID] = struct{}{}
				}
				continue
			}
			for _, userID := range req.userIDs {
				busy[userID] = req.id
			}
			active[req.id] = req
			req.granted <- struct{}{}
		}
		for i := len(remaining); i < previousLength; i++ {
			pending[i] = nil
		}
		pending = remaining
	}

	for cmd := range a.commands {
		switch cmd.kind {
		case accountWriteAdmissionAcquire:
			pending = append(pending, cmd.request)
			grantReady()
		case accountWriteAdmissionRelease:
			if req, ok := active[cmd.id]; ok {
				delete(active, cmd.id)
				for _, userID := range req.userIDs {
					if busy[userID] == cmd.id {
						delete(busy, userID)
					}
				}
				grantReady()
			}
		case accountWriteAdmissionCancel:
			if req, ok := active[cmd.id]; ok {
				delete(active, cmd.id)
				for _, userID := range req.userIDs {
					if busy[userID] == cmd.id {
						delete(busy, userID)
					}
				}
			} else {
				for i, req := range pending {
					if req.id != cmd.id {
						continue
					}
					copy(pending[i:], pending[i+1:])
					pending[len(pending)-1] = nil
					pending = pending[:len(pending)-1]
					break
				}
			}
			grantReady()
			cmd.ack <- struct{}{}
		}
	}
}

func normalizeAccountWriteUserIDs(userIDs []int64) []int64 {
	normalized := make([]int64, 0, len(userIDs))
	for _, userID := range userIDs {
		if userID > 0 {
			normalized = append(normalized, userID)
		}
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i] < normalized[j] })
	if len(normalized) < 2 {
		return normalized
	}
	write := 1
	for read := 1; read < len(normalized); read++ {
		if normalized[read] == normalized[write-1] {
			continue
		}
		normalized[write] = normalized[read]
		write++
	}
	return normalized[:write]
}
