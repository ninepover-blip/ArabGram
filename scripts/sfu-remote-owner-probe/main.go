package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"telesrv/internal/sfu"
	"telesrv/internal/store/redisstore"
)

func main() {
	redisAddr := flag.String("redis-addr", "", "Redis address")
	redisPassword := flag.String("redis-password", "", "Redis password")
	redisDB := flag.Int("redis-db", 0, "Redis DB")
	controlToken := flag.String("control-token", "", "SFU control bearer token")
	controlTLSCAFile := flag.String("control-grpc-tls-ca-file", "", "Root CA bundle for SFU gRPC control calls")
	controlTLSServerName := flag.String("control-grpc-tls-server-name", "", "Server name for SFU gRPC control certificate validation")
	controlTLSClientCertFile := flag.String("control-grpc-tls-client-cert-file", "", "Client certificate for SFU gRPC control mTLS calls")
	controlTLSClientKeyFile := flag.String("control-grpc-tls-client-key-file", "", "Client private key for SFU gRPC control mTLS calls")
	expectRaw := flag.String("expect-instances", "", "comma-separated SFU instance IDs that may own the probe call")
	forbidRaw := flag.String("forbid-instances", "", "comma-separated SFU instance IDs that must not own the probe call")
	runCapacityProbe := flag.Bool("run-capacity-probe", false, "also verify a full selected owner is skipped for a new call")
	runOwnerTTLProbe := flag.Bool("run-owner-ttl-probe", false, "also verify remote owner heartbeat refreshes owners past their TTL")
	ownerTTL := flag.Duration("owner-ttl", 0, "owner record TTL for the probe; defaults to timeout")
	callID := flag.Int64("call-id", 0, "probe call ID; generated when omitted")
	userID := flag.Int64("user-id", 700001, "probe user ID")
	secondUserID := flag.Int64("second-user-id", 0, "second probe user ID; generated as user-id+1 when omitted")
	timeout := flag.Duration("timeout", 10*time.Second, "overall probe timeout")
	flag.Parse()

	if strings.TrimSpace(*redisAddr) == "" {
		fatalf("-redis-addr is required")
	}
	if strings.TrimSpace(*controlToken) == "" {
		fatalf("-control-token is required")
	}
	expect := splitCSV(*expectRaw)
	if len(expect) == 0 {
		fatalf("-expect-instances is required")
	}
	forbid := stringSet(splitCSV(*forbidRaw))
	if *timeout <= 0 {
		fatalf("-timeout must be positive")
	}
	if *ownerTTL <= 0 {
		*ownerTTL = *timeout
	}
	if *callID <= 0 {
		*callID = 970000000000 + time.Now().UnixMilli()%1000000000
	}
	if *userID <= 0 {
		fatalf("-user-id must be positive")
	}
	if *secondUserID <= 0 {
		*secondUserID = *userID + 1
	}
	if *secondUserID == *userID {
		fatalf("-second-user-id must differ from -user-id")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	client, err := redisstore.Open(ctx, *redisAddr, *redisPassword, *redisDB)
	if err != nil {
		fatalf("connect redis: %v", err)
	}
	defer func() { _ = client.Close() }()

	instances := sfu.NewRedisInstanceRegistry(client)
	owners := sfu.NewRedisOwnerRegistry(client)
	remote, closeRemote, err := newRemoteControl(controlConfig{
		token:             *controlToken,
		tlsCAFile:         *controlTLSCAFile,
		tlsServerName:     *controlTLSServerName,
		tlsClientCertFile: *controlTLSClientCertFile,
		tlsClientKeyFile:  *controlTLSClientKeyFile,
	})
	if err != nil {
		fatalf("init remote control: %v", err)
	}
	defer func() { _ = closeRemote() }()
	selector := sfu.NewRegistryOwnerSelector(instances,
		sfu.WithInstanceHealthChecker(remote),
		sfu.WithInstanceHealthTimeout(*timeout/2),
	)
	core, err := sfu.NewRemoteOwnerService(owners, "probe-core", *ownerTTL,
		sfu.WithOwnerSelector(selector),
		sfu.WithRemoteService(remote),
	)
	if err != nil {
		fatalf("create owner service: %v", err)
	}

	records := waitExpectedInstances(ctx, instances, expect)
	answer := joinOrFatal(ctx, core, *callID, *userID, 111111)
	owner, found, err := owners.Get(ctx, *callID)
	if err != nil {
		_ = core.CloseRoom(context.Background(), *callID)
		fatalf("get owner after join: %v", err)
	}
	if !found {
		_ = core.CloseRoom(context.Background(), *callID)
		fatalf("owner record missing after join")
	}
	record, ok := recordByInstance(records, owner.InstanceID)
	if !ok {
		_ = core.CloseRoom(context.Background(), *callID)
		fatalf("owner %s not in expected instances %s", owner.InstanceID, strings.Join(expect, ","))
	}
	if _, forbidden := forbid[owner.InstanceID]; forbidden {
		_ = core.CloseRoom(context.Background(), *callID)
		fatalf("owner %s is forbidden; expected health/capacity selection to avoid it", owner.InstanceID)
	}
	candidate := answer.Candidates[0]
	if record.UDPPort > 0 && candidate.Port != record.UDPPort {
		_ = core.CloseRoom(context.Background(), *callID)
		fatalf("answer candidate port=%d, owner udp=%d", candidate.Port, record.UDPPort)
	}
	if record.AdvertiseIP != "" && candidate.IP != record.AdvertiseIP {
		_ = core.CloseRoom(context.Background(), *callID)
		fatalf("answer candidate ip=%s, owner advertise_ip=%s", candidate.IP, record.AdvertiseIP)
	}
	secondAnswer := joinOrFatal(ctx, core, *callID, *secondUserID, 222222)
	secondOwner, found, err := owners.Get(ctx, *callID)
	if err != nil {
		_ = core.CloseRoom(context.Background(), *callID)
		fatalf("get owner after second join: %v", err)
	}
	if !found {
		_ = core.CloseRoom(context.Background(), *callID)
		fatalf("owner record missing after second join")
	}
	if secondOwner.InstanceID != owner.InstanceID || secondOwner.ControlAddr != owner.ControlAddr {
		_ = core.CloseRoom(context.Background(), *callID)
		fatalf("same call split across owners: first=%s/%s second=%s/%s",
			owner.InstanceID, owner.ControlAddr, secondOwner.InstanceID, secondOwner.ControlAddr)
	}
	secondCandidate := secondAnswer.Candidates[0]
	if secondCandidate.IP != candidate.IP || secondCandidate.Port != candidate.Port {
		_ = core.CloseRoom(context.Background(), *callID)
		fatalf("same call returned different media candidate: first=%s:%d second=%s:%d",
			candidate.IP, candidate.Port, secondCandidate.IP, secondCandidate.Port)
	}
	if err := core.Leave(ctx, *callID, *userID, sfu.EndpointMain); err != nil {
		_ = core.CloseRoom(context.Background(), *callID)
		fatalf("remote owner leave: %v", err)
	}
	if err := core.Leave(ctx, *callID, *secondUserID, sfu.EndpointMain); err != nil {
		_ = core.CloseRoom(context.Background(), *callID)
		fatalf("remote owner second leave: %v", err)
	}
	if err := core.CloseRoom(ctx, *callID); err != nil {
		fatalf("remote owner close room: %v", err)
	}
	if _, found, err := owners.Get(ctx, *callID); err != nil || found {
		fatalf("owner after close found=%v err=%v, want released", found, err)
	}

	capacitySummary := ""
	if *runCapacityProbe {
		firstOwner, secondOwner := runCapacityOrFatal(ctx, core, instances, owners, *callID+1000, *callID+1001, *userID, records)
		capacitySummary = fmt.Sprintf(" capacity_first_owner=%s capacity_second_owner=%s", firstOwner.InstanceID, secondOwner.InstanceID)
	}
	ttlSummary := ""
	if *runOwnerTTLProbe {
		ttlOwner := runOwnerTTLOrFatal(ctx, remote, instances, owners, *ownerTTL, *callID+2000, *userID, records)
		ttlSummary = fmt.Sprintf(" ttl_owner=%s ttl_seconds=%.3f", ttlOwner.InstanceID, (*ownerTTL).Seconds())
	}
	fmt.Printf("sfu remote owner probe ok: redis=%s db=%d control_plane=grpc call_id=%d users=%d,%d owner=%s control=%s candidate=%s:%d%s%s\n",
		*redisAddr, *redisDB, *callID, *userID, *secondUserID, owner.InstanceID, owner.ControlAddr, candidate.IP, candidate.Port, capacitySummary, ttlSummary)
}

type remoteControl interface {
	sfu.RemoteService
	sfu.InstanceHealthChecker
}

type controlConfig struct {
	token             string
	tlsCAFile         string
	tlsServerName     string
	tlsClientCertFile string
	tlsClientKeyFile  string
}

func newRemoteControl(cfg controlConfig) (remoteControl, func() error, error) {
	remote, err := sfu.NewGRPCRemoteService(sfu.GRPCRemoteConfig{
		Token:         cfg.token,
		TLSCAFile:     cfg.tlsCAFile,
		TLSServerName: cfg.tlsServerName,
		TLSCertFile:   cfg.tlsClientCertFile,
		TLSKeyFile:    cfg.tlsClientKeyFile,
	})
	if err != nil {
		return nil, nil, err
	}
	return remote, remote.Close, nil
}

func joinOrFatal(ctx context.Context, service sfu.Service, callID, userID int64, audioSSRC uint32) sfu.ServerAnswer {
	answer, err := service.Join(ctx, callID, userID, sfu.EndpointMain, sfu.ClientOffer{
		AudioSSRC: audioSSRC,
		Ufrag:     fmt.Sprintf("probeufrag%d", userID),
		Pwd:       fmt.Sprintf("probe-password-%d-1234567890", userID),
	})
	if err != nil {
		_ = service.CloseRoom(context.Background(), callID)
		fatalf("remote owner join user=%d: %v", userID, err)
	}
	if answer.Ufrag == "" || answer.Pwd == "" || answer.FingerprintSHA256 == "" || len(answer.Candidates) == 0 {
		_ = service.CloseRoom(context.Background(), callID)
		fatalf("remote owner join user=%d returned incomplete answer: %+v", userID, answer)
	}
	return answer
}

func runCapacityOrFatal(
	ctx context.Context,
	service sfu.Service,
	instances sfu.InstanceRegistry,
	owners sfu.OwnerRegistry,
	firstCallID int64,
	secondCallID int64,
	userID int64,
	records []sfu.InstanceRecord,
) (sfu.OwnerRecord, sfu.OwnerRecord) {
	if len(records) < 2 {
		fatalf("capacity probe requires at least two expected instances, got %d", len(records))
	}
	firstAnswer := joinOrFatal(ctx, service, firstCallID, userID+10, 333333)
	firstOwner, found, err := owners.Get(ctx, firstCallID)
	if err != nil {
		_ = service.CloseRoom(context.Background(), firstCallID)
		fatalf("capacity probe get first owner: %v", err)
	}
	if !found {
		_ = service.CloseRoom(context.Background(), firstCallID)
		fatalf("capacity probe first owner missing")
	}
	firstRecord, ok := recordByInstance(records, firstOwner.InstanceID)
	if !ok || firstRecord.MaxCalls <= 0 {
		_ = service.CloseRoom(context.Background(), firstCallID)
		fatalf("capacity probe first owner %s must have positive max_calls, record=%+v", firstOwner.InstanceID, firstRecord)
	}
	assertCandidateMatchesRecordOrFatal(service, firstCallID, firstAnswer.Candidates[0], firstRecord)
	waitInstanceFullOrFatal(ctx, instances, firstOwner.InstanceID)

	secondAnswer := joinOrFatal(ctx, service, secondCallID, userID+11, 444444)
	secondOwner, found, err := owners.Get(ctx, secondCallID)
	if err != nil {
		_ = service.CloseRoom(context.Background(), firstCallID)
		_ = service.CloseRoom(context.Background(), secondCallID)
		fatalf("capacity probe get second owner: %v", err)
	}
	if !found {
		_ = service.CloseRoom(context.Background(), firstCallID)
		_ = service.CloseRoom(context.Background(), secondCallID)
		fatalf("capacity probe second owner missing")
	}
	if secondOwner.InstanceID == firstOwner.InstanceID {
		_ = service.CloseRoom(context.Background(), firstCallID)
		_ = service.CloseRoom(context.Background(), secondCallID)
		fatalf("capacity probe selected full owner for new call: %s", firstOwner.InstanceID)
	}
	secondRecord, ok := recordByInstance(records, secondOwner.InstanceID)
	if !ok {
		_ = service.CloseRoom(context.Background(), firstCallID)
		_ = service.CloseRoom(context.Background(), secondCallID)
		fatalf("capacity probe second owner %s not in expected records", secondOwner.InstanceID)
	}
	assertCandidateMatchesRecordOrFatal(service, secondCallID, secondAnswer.Candidates[0], secondRecord)
	if err := service.CloseRoom(ctx, firstCallID); err != nil {
		_ = service.CloseRoom(context.Background(), secondCallID)
		fatalf("capacity probe close first call: %v", err)
	}
	if err := service.CloseRoom(ctx, secondCallID); err != nil {
		fatalf("capacity probe close second call: %v", err)
	}
	waitInstanceActiveCallsOrFatal(ctx, instances, firstOwner.InstanceID, 0)
	waitInstanceActiveCallsOrFatal(ctx, instances, secondOwner.InstanceID, 0)
	if _, found, err := owners.Get(ctx, firstCallID); err != nil || found {
		fatalf("capacity probe first owner after close found=%v err=%v", found, err)
	}
	if _, found, err := owners.Get(ctx, secondCallID); err != nil || found {
		fatalf("capacity probe second owner after close found=%v err=%v", found, err)
	}
	return firstOwner, secondOwner
}

func runOwnerTTLOrFatal(
	ctx context.Context,
	remote remoteControl,
	instances sfu.InstanceRegistry,
	owners sfu.OwnerRegistry,
	ownerTTL time.Duration,
	callID int64,
	userID int64,
	records []sfu.InstanceRecord,
) sfu.OwnerRecord {
	if ownerTTL <= 0 {
		fatalf("owner ttl probe requires positive owner TTL")
	}
	heartbeatCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go sfu.RunRemoteOwnerHeartbeat(heartbeatCtx, owners, instances, remote, ownerTTL, ownerTTL/3, ownerTTL/3)
	selector := sfu.NewRegistryOwnerSelector(instances,
		sfu.WithInstanceHealthChecker(remote),
		sfu.WithInstanceHealthTimeout(ownerTTL/3),
	)
	core, err := sfu.NewRemoteOwnerService(owners, "probe-core-ttl", ownerTTL,
		sfu.WithOwnerSelector(selector),
		sfu.WithRemoteService(remote),
	)
	if err != nil {
		fatalf("owner ttl probe owner service: %v", err)
	}
	firstAnswer := joinOrFatal(ctx, core, callID, userID+20, 555555)
	firstOwner, found, err := owners.Get(ctx, callID)
	if err != nil {
		_ = core.CloseRoom(context.Background(), callID)
		fatalf("owner ttl probe get first owner: %v", err)
	}
	if !found {
		_ = core.CloseRoom(context.Background(), callID)
		fatalf("owner ttl probe first owner missing")
	}
	firstRecord, ok := recordByInstance(records, firstOwner.InstanceID)
	if !ok {
		_ = core.CloseRoom(context.Background(), callID)
		fatalf("owner ttl probe owner %s not in expected records", firstOwner.InstanceID)
	}
	assertCandidateMatchesRecordOrFatal(core, callID, firstAnswer.Candidates[0], firstRecord)
	select {
	case <-ctx.Done():
		_ = core.CloseRoom(context.Background(), callID)
		fatalf("owner ttl probe timed out waiting past ttl: %v", ctx.Err())
	case <-time.After(ownerTTL + ownerTTL/2):
	}
	refreshedOwner, found, err := owners.Get(ctx, callID)
	if err != nil {
		_ = core.CloseRoom(context.Background(), callID)
		fatalf("owner ttl probe get refreshed owner: %v", err)
	}
	if !found {
		_ = core.CloseRoom(context.Background(), callID)
		fatalf("owner ttl probe owner expired despite heartbeat")
	}
	if refreshedOwner.InstanceID != firstOwner.InstanceID || refreshedOwner.ControlAddr != firstOwner.ControlAddr {
		_ = core.CloseRoom(context.Background(), callID)
		fatalf("owner ttl probe owner changed: first=%s/%s refreshed=%s/%s",
			firstOwner.InstanceID, firstOwner.ControlAddr, refreshedOwner.InstanceID, refreshedOwner.ControlAddr)
	}
	secondAnswer := joinOrFatal(ctx, core, callID, userID+21, 666666)
	secondOwner, found, err := owners.Get(ctx, callID)
	if err != nil {
		_ = core.CloseRoom(context.Background(), callID)
		fatalf("owner ttl probe get second owner: %v", err)
	}
	if !found || secondOwner.InstanceID != firstOwner.InstanceID || secondOwner.ControlAddr != firstOwner.ControlAddr {
		_ = core.CloseRoom(context.Background(), callID)
		fatalf("owner ttl probe same call was not sticky after ttl: found=%v first=%+v second=%+v", found, firstOwner, secondOwner)
	}
	if secondAnswer.Candidates[0].IP != firstAnswer.Candidates[0].IP || secondAnswer.Candidates[0].Port != firstAnswer.Candidates[0].Port {
		_ = core.CloseRoom(context.Background(), callID)
		fatalf("owner ttl probe candidate changed after ttl: first=%s:%d second=%s:%d",
			firstAnswer.Candidates[0].IP, firstAnswer.Candidates[0].Port,
			secondAnswer.Candidates[0].IP, secondAnswer.Candidates[0].Port)
	}
	if err := core.CloseRoom(ctx, callID); err != nil {
		fatalf("owner ttl probe close room: %v", err)
	}
	waitInstanceActiveCallsOrFatal(ctx, instances, firstOwner.InstanceID, 0)
	return firstOwner
}

func waitInstanceFullOrFatal(ctx context.Context, registry sfu.InstanceRegistry, instanceID string) {
	var last sfu.InstanceRecord
	for ctx.Err() == nil {
		records, err := registry.List(ctx)
		if err != nil {
			fatalf("capacity probe list sfu instances: %v", err)
		}
		for _, record := range records {
			if strings.TrimSpace(record.InstanceID) != strings.TrimSpace(instanceID) {
				continue
			}
			last = record
			if record.MaxCalls > 0 && record.ActiveCalls >= record.MaxCalls {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	fatalf("capacity probe owner %s did not become full before timeout; last_record=%+v", instanceID, last)
}

func waitInstanceActiveCallsOrFatal(ctx context.Context, registry sfu.InstanceRegistry, instanceID string, want int) {
	var last sfu.InstanceRecord
	for ctx.Err() == nil {
		records, err := registry.List(ctx)
		if err != nil {
			fatalf("capacity probe list sfu instances after close: %v", err)
		}
		for _, record := range records {
			if strings.TrimSpace(record.InstanceID) != strings.TrimSpace(instanceID) {
				continue
			}
			last = record
			if record.ActiveCalls == want {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	fatalf("capacity probe owner %s active_calls did not become %d before timeout; last_record=%+v", instanceID, want, last)
}

func assertCandidateMatchesRecordOrFatal(service sfu.Service, callID int64, candidate sfu.Candidate, record sfu.InstanceRecord) {
	if record.UDPPort > 0 && candidate.Port != record.UDPPort {
		_ = service.CloseRoom(context.Background(), callID)
		fatalf("capacity probe candidate port=%d, owner udp=%d", candidate.Port, record.UDPPort)
	}
	if record.AdvertiseIP != "" && candidate.IP != record.AdvertiseIP {
		_ = service.CloseRoom(context.Background(), callID)
		fatalf("capacity probe candidate ip=%s, owner advertise_ip=%s", candidate.IP, record.AdvertiseIP)
	}
}

func waitExpectedInstances(ctx context.Context, registry sfu.InstanceRegistry, expect []string) []sfu.InstanceRecord {
	var last []sfu.InstanceRecord
	for ctx.Err() == nil {
		records, err := registry.List(ctx)
		if err != nil {
			fatalf("list sfu instances: %v", err)
		}
		last = records
		if missing := missingInstances(records, expect); len(missing) == 0 {
			return filterRecords(records, expect)
		}
		time.Sleep(200 * time.Millisecond)
	}
	fatalf("sfu remote owner probe missing instances %s; last_records=%s", strings.Join(missingInstances(last, expect), ","), describeRecords(last))
	return nil
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func stringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func missingInstances(records []sfu.InstanceRecord, expect []string) []string {
	seen := make(map[string]struct{}, len(records))
	for _, record := range records {
		seen[strings.TrimSpace(record.InstanceID)] = struct{}{}
	}
	var missing []string
	for _, id := range expect {
		if _, ok := seen[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing
}

func filterRecords(records []sfu.InstanceRecord, expect []string) []sfu.InstanceRecord {
	want := make(map[string]struct{}, len(expect))
	for _, id := range expect {
		want[id] = struct{}{}
	}
	out := make([]sfu.InstanceRecord, 0, len(expect))
	for _, record := range records {
		if _, ok := want[strings.TrimSpace(record.InstanceID)]; ok {
			out = append(out, record)
		}
	}
	return out
}

func recordByInstance(records []sfu.InstanceRecord, instanceID string) (sfu.InstanceRecord, bool) {
	for _, record := range records {
		if strings.TrimSpace(record.InstanceID) == strings.TrimSpace(instanceID) {
			return record, true
		}
	}
	return sfu.InstanceRecord{}, false
}

func describeRecords(records []sfu.InstanceRecord) string {
	if len(records) == 0 {
		return "<empty>"
	}
	parts := make([]string, 0, len(records))
	for _, record := range records {
		parts = append(parts, fmt.Sprintf("%s(%s,advertise=%s,udp=%d,active=%d,max=%d)",
			record.InstanceID, record.ControlAddr, record.AdvertiseIP, record.UDPPort, record.ActiveCalls, record.MaxCalls))
	}
	sort.Strings(parts)
	return strings.Join(parts, ";")
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
