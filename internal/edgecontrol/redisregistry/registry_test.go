package redisregistry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"telesrv/internal/edgecontrol"
)

func TestRegistryValidatesInputs(t *testing.T) {
	reg := New(nil)
	if err := reg.AcquireInstanceLease(context.Background(), "edge-a", "lease-a", time.Minute); err != ErrNilClient {
		t.Fatalf("AcquireInstanceLease nil client err = %v, want ErrNilClient", err)
	}

	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	reg = New(c)
	defer c.Close()
	if err := reg.AcquireInstanceLease(context.Background(), "", "", 0); err != ErrInvalidRegistry {
		t.Fatalf("AcquireInstanceLease invalid err = %v, want ErrInvalidRegistry", err)
	}
	for _, invalid := range []string{" edge-a", "edge-a ", strings.Repeat("e", edgecontrol.MaxDeliveryInstanceIDBytes+1)} {
		if err := reg.AcquireInstanceLease(context.Background(), invalid, "lease-a", time.Minute); err != ErrInvalidRegistry {
			t.Fatalf("AcquireInstanceLease instance %q err = %v, want ErrInvalidRegistry", invalid, err)
		}
	}
	for _, invalid := range []string{" lease-a", "lease-a ", strings.Repeat("l", maxLocationLeaseIDBytes+1)} {
		if err := reg.AcquireInstanceLease(context.Background(), "edge-a", invalid, time.Minute); err != ErrInvalidRegistry {
			t.Fatalf("AcquireInstanceLease lease %q err = %v, want ErrInvalidRegistry", invalid, err)
		}
	}
	if err := reg.ApplyLocationMutations(context.Background(), "edge-a", "lease-a", []edgecontrol.LocationMutation{{}}); err != ErrInvalidRecord {
		t.Fatalf("ApplyLocationMutations invalid err = %v, want ErrInvalidRecord", err)
	}
	if _, err := reg.ListUser(context.Background(), 0); err != ErrInvalidRegistry {
		t.Fatalf("ListUser invalid err = %v, want ErrInvalidRegistry", err)
	}
}

func TestRegistryLeaseMutationListAndReleaseIntegration(t *testing.T) {
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis integration test")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()

	prefix := fmt.Sprintf("test:edge-location:%d", time.Now().UnixNano())
	now := time.Unix(1_800_000_000, 0)
	reg := New(client, WithPrefix(prefix), WithNow(func() time.Time { return now }))
	defer func() {
		keys, _ := client.Keys(ctx, prefix+"*").Result()
		if len(keys) > 0 {
			_ = client.Del(ctx, keys...).Err()
		}
	}()

	const (
		instanceID = "edge-a"
		leaseID    = "lease-a"
	)
	if err := reg.AcquireInstanceLease(ctx, instanceID, leaseID, time.Minute); err != nil {
		t.Fatalf("AcquireInstanceLease err = %v", err)
	}
	if err := reg.AcquireInstanceLease(ctx, instanceID, "competing-lease", time.Minute); !errors.Is(err, edgecontrol.ErrLocationLeaseHeld) {
		t.Fatalf("competing AcquireInstanceLease err = %v, want ErrLocationLeaseHeld", err)
	}

	rawAuthKeyID := [8]byte{1, 2, 3}
	businessAuthKeyID := [8]byte{7, 8, 9}
	record := edgecontrol.LocationRecord{
		InstanceID:        instanceID,
		UserID:            1000000002,
		RawAuthKeyID:      rawAuthKeyID,
		BusinessAuthKeyID: businessAuthKeyID,
		SessionID:         44,
		ReceivesUpdates:   true,
		Layer:             228,
		ActiveChannelIDs:  []int64{77},
		ChannelSubscriptions: []edgecontrol.ChannelSubscriptionLocation{{
			ChannelID:         88,
			ExpiresAtUnixNano: now.Add(time.Minute).UnixNano(),
		}},
	}
	if err := reg.ApplyLocationMutations(ctx, instanceID, "wrong-lease", []edgecontrol.LocationMutation{{Record: record}}); !errors.Is(err, edgecontrol.ErrLocationLeaseLost) {
		t.Fatalf("wrong-lease mutation err = %v, want ErrLocationLeaseLost", err)
	}
	if err := reg.ApplyLocationMutations(ctx, instanceID, leaseID, []edgecontrol.LocationMutation{{Record: record}}); err != nil {
		t.Fatalf("ApplyLocationMutations err = %v", err)
	}
	membership := edgecontrol.ChannelMembershipRecord{InstanceID: instanceID, UserID: record.UserID, ChannelIDs: []int64{77}}
	if err := reg.ApplyChannelMembershipMutations(ctx, instanceID, "wrong-lease", []edgecontrol.ChannelMembershipMutation{{Record: membership}}); !errors.Is(err, edgecontrol.ErrLocationLeaseLost) {
		t.Fatalf("wrong-lease membership mutation err = %v, want ErrLocationLeaseLost", err)
	}
	if err := reg.ApplyChannelMembershipMutations(ctx, instanceID, leaseID, []edgecontrol.ChannelMembershipMutation{{Record: membership}}); err != nil {
		t.Fatalf("ApplyChannelMembershipMutations err = %v", err)
	}
	assertLocationIndexes(t, ctx, reg, record, membership)

	secondRecord := record
	secondRecord.RawAuthKeyID = [8]byte{4, 5, 6}
	secondRecord.BusinessAuthKeyID = [8]byte{9, 8, 7}
	secondRecord.SessionID++
	if err := reg.ApplyLocationMutations(ctx, instanceID, leaseID, []edgecontrol.LocationMutation{{Record: secondRecord}}); err != nil {
		t.Fatalf("publish second session location: %v", err)
	}
	if refs, err := client.SCard(ctx, reg.channelMemberKey(77)).Result(); err != nil || refs != 1 {
		t.Fatalf("channel member refs with two sessions = %d err=%v, want one (instance,user) ref", refs, err)
	}
	membership.ChannelIDs = []int64{99}
	if err := reg.ApplyChannelMembershipMutations(ctx, instanceID, leaseID, []edgecontrol.ChannelMembershipMutation{{Record: membership}}); err != nil {
		t.Fatalf("update channel membership mutation: %v", err)
	}
	if old, err := reg.ListChannelMember(ctx, 77); err != nil || len(old) != 0 {
		t.Fatalf("old channel membership after update = %+v err=%v, want empty", old, err)
	}
	if current, err := reg.ListChannelMember(ctx, 99); err != nil || len(current) != 1 || current[0].UserID != membership.UserID {
		t.Fatalf("new channel membership after update = %+v err=%v, want user %d", current, err, membership.UserID)
	}
	membership.ChannelIDs = []int64{77}
	if err := reg.ApplyChannelMembershipMutations(ctx, instanceID, leaseID, []edgecontrol.ChannelMembershipMutation{{Record: membership}}); err != nil {
		t.Fatalf("restore channel membership mutation: %v", err)
	}
	if err := reg.ApplyLocationMutations(ctx, instanceID, leaseID, []edgecontrol.LocationMutation{{Record: secondRecord, Deleted: true}}); err != nil {
		t.Fatalf("delete second session location: %v", err)
	}

	oldUserID := record.UserID
	record.UserID++
	record.ActiveChannelIDs = []int64{99}
	if err := reg.ApplyLocationMutations(ctx, instanceID, leaseID, []edgecontrol.LocationMutation{{Record: record}}); err != nil {
		t.Fatalf("update location mutation err = %v", err)
	}
	if old, err := reg.ListUser(ctx, oldUserID); err != nil || len(old) != 0 {
		t.Fatalf("old user index after update = %+v err=%v, want empty", old, err)
	}
	if old, err := reg.ListChannelInterest(ctx, 77); err != nil || len(old) != 0 {
		t.Fatalf("old channel index after update = %+v err=%v, want empty", old, err)
	}
	if current, err := reg.ListUser(ctx, record.UserID); err != nil || len(current) != 1 {
		t.Fatalf("new user index after update = %+v err=%v, want one", current, err)
	}
	if err := reg.ApplyLocationMutations(ctx, instanceID, leaseID, []edgecontrol.LocationMutation{{Record: record, Deleted: true}}); err != nil {
		t.Fatalf("delete location mutation err = %v", err)
	}
	if current, err := reg.ListUser(ctx, record.UserID); err != nil || len(current) != 0 {
		t.Fatalf("user index after delete = %+v err=%v, want empty", current, err)
	}
	if active, err := reg.ListActiveRawAuthKeyIDs(ctx); err != nil || len(active) != 0 {
		t.Fatalf("active raw keys after delete = %v err=%v, want empty", active, err)
	}
	if err := reg.ApplyLocationMutations(ctx, instanceID, leaseID, []edgecontrol.LocationMutation{{Record: record}}); err != nil {
		t.Fatalf("restore location before release err = %v", err)
	}

	if err := reg.RenewInstanceLease(ctx, instanceID, leaseID, time.Minute); err != nil {
		t.Fatalf("RenewInstanceLease err = %v", err)
	}
	if err := reg.ReleaseInstanceLease(ctx, instanceID, leaseID); err != nil {
		t.Fatalf("ReleaseInstanceLease err = %v", err)
	}
	if records, err := reg.ListUser(ctx, record.UserID); err != nil || len(records) != 0 {
		t.Fatalf("ListUser after release = %+v err=%v, want empty", records, err)
	}
	if records, err := reg.ListChannelMember(ctx, 77); err != nil || len(records) != 0 {
		t.Fatalf("ListChannelMember after release = %+v err=%v, want empty", records, err)
	}
	if active, err := reg.ListActiveRawAuthKeyIDs(ctx); err != nil || len(active) != 0 {
		t.Fatalf("ListActiveRawAuthKeyIDs after release = %v err=%v, want empty", active, err)
	}

	const expiringInstance = "edge-expiring"
	const expiringLease = "lease-expiring"
	expiring := record
	expiring.InstanceID = expiringInstance
	expiring.UserID++
	expiring.SessionID++
	if err := reg.AcquireInstanceLease(ctx, expiringInstance, expiringLease, 50*time.Millisecond); err != nil {
		t.Fatalf("acquire expiring lease: %v", err)
	}
	if err := reg.ApplyLocationMutations(ctx, expiringInstance, expiringLease, []edgecontrol.LocationMutation{{Record: expiring}}); err != nil {
		t.Fatalf("publish expiring location: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if records, err := reg.ListUser(ctx, expiring.UserID); err != nil || len(records) != 0 {
		t.Fatalf("ListUser after lease expiry = %+v err=%v, want empty", records, err)
	}
	if err := reg.RenewInstanceLease(ctx, expiringInstance, expiringLease, time.Minute); !errors.Is(err, edgecontrol.ErrLocationLeaseLost) {
		t.Fatalf("renew expired instance err = %v, want ErrLocationLeaseLost", err)
	}
}

func assertLocationIndexes(t *testing.T, ctx context.Context, reg *Registry, record edgecontrol.LocationRecord, membership edgecontrol.ChannelMembershipRecord) {
	t.Helper()
	byUser, err := reg.ListUser(ctx, record.UserID)
	if err != nil || len(byUser) != 1 || byUser[0].SessionID != record.SessionID {
		t.Fatalf("ListUser = %+v err=%v, want session %d", byUser, err, record.SessionID)
	}
	if byUser[0].LocationRevision <= 0 {
		t.Fatalf("ListUser location revision = %d, want positive Redis revision", byUser[0].LocationRevision)
	}
	byUsers, err := reg.ListUsers(ctx, []int64{record.UserID, 0, record.UserID, 999999})
	if err != nil || len(byUsers[record.UserID]) != 1 || len(byUsers[999999]) != 0 {
		t.Fatalf("ListUsers = %+v err=%v", byUsers, err)
	}
	byAuth, err := reg.ListBusinessAuthKey(ctx, record.BusinessAuthKeyID)
	if err != nil || len(byAuth) != 1 {
		t.Fatalf("ListBusinessAuthKey = %+v err=%v", byAuth, err)
	}
	byInstance, err := reg.ListInstance(ctx, record.InstanceID)
	if err != nil || len(byInstance) != 1 {
		t.Fatalf("ListInstance = %+v err=%v", byInstance, err)
	}
	byInterest, err := reg.ListChannelInterest(ctx, 77)
	if err != nil || len(byInterest) != 1 {
		t.Fatalf("ListChannelInterest = %+v err=%v", byInterest, err)
	}
	byMember, err := reg.ListChannelMember(ctx, membership.ChannelIDs[0])
	if err != nil || len(byMember) != 1 || byMember[0].UserID != membership.UserID || byMember[0].InstanceID != membership.InstanceID {
		t.Fatalf("ListChannelMember = %+v err=%v", byMember, err)
	}
	bySubscription, err := reg.ListChannelSubscription(ctx, 88)
	if err != nil || len(bySubscription) != 1 {
		t.Fatalf("ListChannelSubscription = %+v err=%v", bySubscription, err)
	}
	targets, err := reg.ListChannelDeliveryTargets(ctx, []edgecontrol.ChannelDeliveryRoute{{
		ChannelID: 77, Audience: edgecontrol.ChannelAudienceMembers,
	}})
	if err != nil || len(targets) != 1 || targets[0] != record.InstanceID {
		t.Fatalf("ListChannelDeliveryTargets members = %v err=%v, want [%s]", targets, err, record.InstanceID)
	}
	targets, err = reg.ListChannelDeliveryTargets(ctx, []edgecontrol.ChannelDeliveryRoute{{
		ChannelID: 88, Audience: edgecontrol.ChannelAudienceMessageBox, AudienceUsers: []int64{record.UserID},
	}})
	if err != nil || len(targets) != 1 || targets[0] != record.InstanceID {
		t.Fatalf("ListChannelDeliveryTargets message-box = %v err=%v, want [%s]", targets, err, record.InstanceID)
	}
	channelIDs, err := reg.ListOnlineChannelIDsSnapshot(ctx)
	if err != nil || len(channelIDs) != 2 || channelIDs[0] != 77 || channelIDs[1] != 88 {
		t.Fatalf("ListOnlineChannelIDsSnapshot = %v err=%v, want [77 88]", channelIDs, err)
	}
	activeRaw, err := reg.ListActiveRawAuthKeyIDs(ctx)
	if err != nil || len(activeRaw) != 1 || activeRaw[0] != record.RawAuthKeyID {
		t.Fatalf("ListActiveRawAuthKeyIDs = %v err=%v, want %v", activeRaw, err, record.RawAuthKeyID)
	}
}
