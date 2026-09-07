package loadharness

import (
	"bufio"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"
)

const (
	deliveryLedgerVersion             = 2
	deliveryRecordHeader              = 80
	deliveryObservationBytes          = 112
	deliveryCacheBytes                = 32 << 20
	defaultDeliveryCacheRecords       = 4096
	defaultDeliveryLedgerBytes  int64 = 32 << 30
)

var deliveryCRC = crc32.MakeTable(crc32.Castagnoli)
var errDeliveryQuota = errors.New("delivery ledger disk quota exhausted")

type deliveryLedgerOptions struct {
	CacheRecords int
	MaxBytes     int64
	// Explicit fixed routes; tests may use a different frozen private topology.
	Targets map[int]int64
}

func (o deliveryLedgerOptions) defaults() deliveryLedgerOptions {
	if o.CacheRecords == 0 {
		o.CacheRecords = defaultDeliveryCacheRecords
	}
	if o.MaxBytes == 0 {
		o.MaxBytes = defaultDeliveryLedgerBytes
	}
	return o
}

func (o deliveryLedgerOptions) validate() error {
	o = o.defaults()
	if o.CacheRecords < 1 || o.CacheRecords > 65536 || o.MaxBytes < deliveryRecordHeader || o.MaxBytes > 1<<40 {
		return errors.New("invalid delivery cache record limit or ledger byte quota")
	}
	return nil
}

type deliveryKey struct {
	sender   int
	sequence uint64
}
type deliveryRoute struct {
	Sender  int   `json:"sender_session_index"`
	Target  int64 `json:"target_user_id"`
	Devices []int `json:"participating_devices"` // Origin first, followed by expected devices.
	offset  int64
	size    int
}
type deliveryRecord struct {
	key          deliveryKey
	expectation  deliveryExpectation
	observations []deliveryObservation // Same immutable ordering as the route.
}

func (r *deliveryRecord) clone() *deliveryRecord {
	copy := *r
	copy.observations = append([]deliveryObservation(nil), r.observations...)
	return &copy
}

type DeliveryLedgerReport struct {
	Version           int       `json:"version"`
	CacheLimitRecords int       `json:"cache_limit_records"`
	CacheLimitBytes   int64     `json:"cache_limit_bytes"`
	CacheRecords      int       `json:"cache_records"`
	CacheBytes        int64     `json:"cache_bytes"`
	PeakCacheRecords  int       `json:"peak_cache_records"`
	PeakCacheBytes    int64     `json:"peak_cache_bytes"`
	FileBytes         int64     `json:"file_bytes"`
	LimitBytes        int64     `json:"limit_bytes"`
	Reads             uint64    `json:"reads"`
	Writes            uint64    `json:"writes"`
	Evictions         uint64    `json:"evictions"`
	AuditComplete     bool      `json:"audit_complete"`
	AuditRecords      uint64    `json:"audit_records"`
	AuditStartedAt    time.Time `json:"audit_started_at"`
	AuditFinishedAt   time.Time `json:"audit_finished_at"`
	AuditDurationMS   float64   `json:"audit_duration_ms"`
	SHA256            string    `json:"sha256,omitempty"`
	Error             string    `json:"error,omitempty"`
}

type deliveryLedger struct {
	f             *os.File
	dir           string
	epoch         time.Time
	devices       map[int]SessionRecord
	routes        []deliveryRoute
	bySender      map[int]int
	stride        int64
	cache         map[deliveryKey]*list.Element
	lru           list.List
	stats         DeliveryLedgerReport
	metadataHash  [32]byte
	metadataBytes int64
}

func newDeliveryLedger(path, runID string, epoch time.Time, devices map[int]SessionRecord, routes []deliveryRoute, options deliveryLedgerOptions) (*deliveryLedger, error) {
	if err := options.validate(); err != nil {
		return nil, err
	}
	options = options.defaults()
	l := &deliveryLedger{dir: path, epoch: epoch, devices: devices, routes: routes, bySender: make(map[int]int), cache: make(map[deliveryKey]*list.Element),
		stats: DeliveryLedgerReport{Version: deliveryLedgerVersion, CacheLimitRecords: options.CacheRecords, CacheLimitBytes: deliveryCacheBytes, LimitBytes: options.MaxBytes}}
	for i := range l.routes {
		r := &l.routes[i]
		r.offset = l.stride
		r.size = deliveryRecordHeader + len(r.Devices)*deliveryObservationBytes
		if cacheRecordBytes(len(r.Devices)) > deliveryCacheBytes {
			return nil, errors.New("delivery route exceeds cache byte budget")
		}
		l.stride += int64(r.size)
		l.bySender[r.Sender] = i
	}
	if l.stride == 0 || l.stride > options.MaxBytes {
		return nil, errors.New("delivery topology exceeds ledger quota")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		return nil, fmt.Errorf("create exclusive delivery ledger: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(path, "records.bin"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	l.f = f
	// This private metadata is enough to interpret the fixed record layout later.
	identities := make(map[int]map[string]int64, len(devices))
	for index, device := range devices {
		identities[index] = map[string]int64{"account_index": int64(device.AccountIndex), "device_index": int64(device.DeviceIndex), "user_id": device.UserID}
	}
	metadata := map[string]any{"version": deliveryLedgerVersion, "record_header_bytes": deliveryRecordHeader, "device_observation_bytes": deliveryObservationBytes, "device_modes": map[string]int{"unavailable": 0, "online": 1, "planned_offline": 2}, "run_id": runID, "epoch": epoch.UTC(), "routes": routes, "devices": identities, "cycle_bytes": l.stride, "limits": l.stats}
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err == nil {
		err = writeFileAtomic(filepath.Join(path, "layout.json"), append(data, '\n'), 0o600)
	}
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	l.metadataHash = sha256.Sum256(append(data, '\n'))
	l.metadataBytes = int64(len(data) + 1)
	return l, nil
}

func cacheRecordBytes(devices int) int64 { return 512 + int64(devices)*256 }

func (l *deliveryLedger) address(key deliveryKey) (deliveryRoute, int64, error) {
	index, ok := l.bySender[key.sender]
	if !ok || key.sequence == 0 {
		return deliveryRoute{}, 0, errors.New("unknown delivery route")
	}
	r := l.routes[index]
	remaining := l.stats.LimitBytes - r.offset - int64(r.size)
	if remaining < 0 || key.sequence-1 > uint64(remaining/l.stride) {
		return r, 0, errDeliveryQuota
	}
	return r, int64(key.sequence-1)*l.stride + r.offset, nil
}

func (l *deliveryLedger) get(key deliveryKey) (*deliveryRecord, bool, error) {
	if element := l.cache[key]; element != nil {
		l.lru.MoveToFront(element)
		return element.Value.(*deliveryRecord), true, nil
	}
	route, offset, err := l.address(key)
	if err != nil {
		return nil, false, err
	}
	if offset >= l.stats.FileBytes {
		return nil, false, nil
	}
	data := make([]byte, route.size)
	l.stats.Reads++
	if _, err := l.f.ReadAt(data, offset); err != nil {
		return nil, false, fmt.Errorf("read delivery record: %w", err)
	}
	record, err := l.decode(data, route, key)
	if err != nil || record == nil {
		return nil, false, err
	}
	l.cachePut(record)
	return record, true, nil
}

func (l *deliveryLedger) put(record *deliveryRecord) error {
	route, offset, err := l.address(record.key)
	if err != nil {
		return err
	}
	data := l.encode(record, route)
	if n, err := l.f.WriteAt(data, offset); err != nil {
		return fmt.Errorf("write delivery record: %w", err)
	} else if n != len(data) {
		return io.ErrShortWrite
	}
	l.stats.Writes++
	l.stats.FileBytes = max(l.stats.FileBytes, offset+int64(len(data)))
	l.cachePut(record)
	return nil
}

func (l *deliveryLedger) cachePut(record *deliveryRecord) {
	if element := l.cache[record.key]; element != nil {
		element.Value = record
		l.lru.MoveToFront(element)
		return
	}
	bytes := cacheRecordBytes(len(record.observations))
	for len(l.cache) >= l.stats.CacheLimitRecords || l.stats.CacheBytes+bytes > l.stats.CacheLimitBytes {
		old := l.lru.Back()
		previous := old.Value.(*deliveryRecord)
		delete(l.cache, previous.key)
		l.lru.Remove(old)
		l.stats.CacheBytes -= cacheRecordBytes(len(previous.observations))
		l.stats.Evictions++
	}
	l.cache[record.key] = l.lru.PushFront(record)
	l.stats.CacheRecords = len(l.cache)
	l.stats.CacheBytes += bytes
	l.stats.PeakCacheRecords = max(l.stats.PeakCacheRecords, len(l.cache))
	l.stats.PeakCacheBytes = max(l.stats.PeakCacheBytes, l.stats.CacheBytes)
}

func (l *deliveryLedger) relative(at time.Time) int64 {
	if at.IsZero() {
		return math.MinInt64
	}
	return int64(at.Sub(l.epoch))
}
func (l *deliveryLedger) absolute(relative int64) time.Time {
	if relative == math.MinInt64 {
		return time.Time{}
	}
	return l.epoch.Add(time.Duration(relative))
}

func (l *deliveryLedger) encode(record *deliveryRecord, route deliveryRoute) []byte {
	data := make([]byte, route.size)
	copy(data, "LD02")
	put := binary.LittleEndian.PutUint64
	put(data[8:], record.key.sequence)
	put(data[16:], uint64(record.expectation.randomID))
	put(data[24:], uint64(l.relative(record.expectation.startedAt)))
	put(data[32:], uint64(l.relative(record.expectation.plannedAt)))
	binary.LittleEndian.PutUint32(data[40:], uint32(record.expectation.senderMessageID))
	binary.LittleEndian.PutUint32(data[44:], uint32(record.expectation.recipientMessageID))
	data[48], data[49] = byte(record.expectation.initialOutcome), byte(record.expectation.retryOutcome)
	if record.expectation.committed {
		data[50] = 1
	}
	binary.LittleEndian.PutUint32(data[52:], uint32(record.key.sender))
	put(data[56:], uint64(route.Target))
	put(data[64:], uint64(l.relative(record.expectation.frozenAt)))
	put(data[72:], record.expectation.stateRevision)
	for i, observation := range record.observations {
		b := data[deliveryRecordHeader+i*deliveryObservationBytes:]
		b[0] = byte(observation.sources)
		put(b[8:], uint64(l.relative(observation.firstAt)))
		put(b[16:], uint64(l.relative(observation.firstLiveAt)))
		put(b[24:], observation.repeats)
		b[1] = byte(observation.mode)
		put(b[32:], observation.expectedGeneration)
		put(b[40:], observation.expectedEpoch)
		put(b[48:], observation.firstGeneration)
		put(b[56:], observation.liveGeneration)
		put(b[64:], observation.differenceGeneration)
		put(b[72:], uint64(l.relative(observation.onlineLiveAt)))
		put(b[80:], observation.firstEpoch)
		put(b[88:], observation.liveEpoch)
		put(b[96:], observation.differenceEpoch)
		put(b[104:], observation.staleObservations)
	}
	binary.LittleEndian.PutUint32(data[4:], crc32.Checksum(data[8:], deliveryCRC))
	return data
}

func (l *deliveryLedger) decode(data []byte, route deliveryRoute, key deliveryKey) (*deliveryRecord, error) {
	zero := true
	for _, value := range data {
		if value != 0 {
			zero = false
			break
		}
	}
	if zero {
		return nil, nil
	} // Never registered sparse slot; totals audit detects erased records.
	get := binary.LittleEndian.Uint64
	if string(data[:4]) != "LD02" || binary.LittleEndian.Uint32(data[4:]) != crc32.Checksum(data[8:], deliveryCRC) || get(data[8:]) != key.sequence || int(binary.LittleEndian.Uint32(data[52:])) != key.sender || int64(get(data[56:])) != route.Target || data[48] > byte(sendUncertain) || data[49] > byte(sendUncertain) || data[50] > 1 {
		return nil, errors.New("delivery record checksum or identity mismatch")
	}
	e := deliveryExpectation{senderSessionIndex: key.sender, senderUserID: l.devices[key.sender].UserID, targetUserID: route.Target, devices: route.Devices[1:], randomID: int64(get(data[16:])),
		startedAt: l.absolute(int64(get(data[24:]))), plannedAt: l.absolute(int64(get(data[32:]))), senderMessageID: int(binary.LittleEndian.Uint32(data[40:])), recipientMessageID: int(binary.LittleEndian.Uint32(data[44:])), initialOutcome: sendAttemptOutcome(data[48]), retryOutcome: sendAttemptOutcome(data[49]), committed: data[50] == 1, frozenAt: l.absolute(int64(get(data[64:]))), stateRevision: get(data[72:])}
	if e.randomID == 0 {
		return nil, errors.New("delivery record has empty intent")
	}
	record := &deliveryRecord{key: key, expectation: e, observations: make([]deliveryObservation, len(route.Devices))}
	for i := range record.observations {
		b := data[deliveryRecordHeader+i*deliveryObservationBytes:]
		if b[0] > byte(deliveryLive|deliveryDifference) {
			return nil, errors.New("invalid delivery evidence source")
		}
		record.observations[i] = deliveryObservation{sources: deliverySource(b[0]), firstAt: l.absolute(int64(get(b[8:]))), firstLiveAt: l.absolute(int64(get(b[16:]))), repeats: get(b[24:]), mode: deliveryDeviceMode(b[1]), expectedGeneration: get(b[32:]), expectedEpoch: get(b[40:]), firstGeneration: get(b[48:]), liveGeneration: get(b[56:]), differenceGeneration: get(b[64:]), onlineLiveAt: l.absolute(int64(get(b[72:]))), firstEpoch: get(b[80:]), liveEpoch: get(b[88:]), differenceEpoch: get(b[96:]), staleObservations: get(b[104:])}
		o := record.observations[i]
		if o.mode > deliveryDeviceOffline || (o.mode == deliveryDeviceOnline && (o.expectedGeneration == 0 || o.expectedEpoch == 0)) || (o.sources != 0 && o.firstGeneration == 0) || (o.sources&deliveryLive != 0 && o.liveGeneration == 0) || (o.sources&deliveryDifference != 0 && o.differenceGeneration == 0) || (!o.onlineLiveAt.IsZero() && (o.mode != deliveryDeviceOnline || o.sources&deliveryLive == 0)) {
			return nil, errors.New("invalid delivery device generation evidence")
		}
	}
	return record, nil
}

// Audit runs after all client handlers stop. It scans the file without filling
// the cache, hashes holes too, and never buffers or sorts the whole run.
func (l *deliveryLedger) verifyLayout() error {
	metadataFile, err := os.Open(filepath.Join(l.dir, "layout.json"))
	if err != nil {
		return err
	}
	metadata, readErr := io.ReadAll(io.LimitReader(metadataFile, l.metadataBytes+1))
	closeErr := metadataFile.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if int64(len(metadata)) != l.metadataBytes || sha256.Sum256(metadata) != l.metadataHash {
		return errors.New("delivery ledger layout changed")
	}
	return nil
}

func (l *deliveryLedger) audit(ctx context.Context, consume func(*deliveryRecord)) error {
	started := time.Now()
	l.stats.AuditStartedAt = started.UTC()
	defer func() {
		l.stats.AuditFinishedAt = time.Now().UTC()
		l.stats.AuditDurationMS = durationMS(time.Since(started))
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := l.verifyLayout(); err != nil {
		return err
	}
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("sync delivery ledger: %w", err)
	}
	stat, err := l.f.Stat()
	if err != nil {
		return err
	}
	if stat.Size() != l.stats.FileBytes {
		return errors.New("delivery ledger size changed")
	}
	if err := l.verifyRecordPath(stat); err != nil {
		return err
	}
	if _, err := l.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	hash := sha256.New()
	reader := bufio.NewReaderSize(io.TeeReader(l.f, hash), 256<<10)
	var offset int64
	var sequence uint64 = 1
	maxSize := 0
	for _, route := range l.routes {
		maxSize = max(maxSize, route.size)
	}
	buffer := make([]byte, maxSize)
	for offset < stat.Size() {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, route := range l.routes {
			if offset == stat.Size() {
				break
			}
			data := buffer[:route.size]
			if _, err := io.ReadFull(reader, data); err != nil {
				return fmt.Errorf("scan delivery ledger: %w", err)
			}
			record, err := l.decode(data, route, deliveryKey{sender: route.Sender, sequence: sequence})
			if err != nil {
				return err
			}
			if record != nil {
				consume(record)
				l.stats.AuditRecords++
			}
			offset += int64(route.size)
		}
		sequence++
	}
	l.stats.SHA256 = hex.EncodeToString(hash.Sum(nil))
	if err := l.verifyRecordPath(stat); err != nil {
		return err
	}
	return l.verifyLayout()
}

func (l *deliveryLedger) verifyRecordPath(openFile os.FileInfo) error {
	published, err := os.Lstat(filepath.Join(l.dir, "records.bin"))
	if err != nil {
		return err
	}
	if !published.Mode().IsRegular() || !os.SameFile(openFile, published) || published.Size() != openFile.Size() || !published.ModTime().Equal(openFile.ModTime()) {
		return errors.New("delivery ledger record path changed")
	}
	return nil
}

func (l *deliveryLedger) close() error {
	if l == nil || l.f == nil {
		return nil
	}
	err := l.f.Close()
	l.f = nil
	return err
}
