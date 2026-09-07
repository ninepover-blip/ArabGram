package loadharness

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/iamxvbaba/td/tg"
)

const (
	fileFixtureVersion    = 1
	fixturePatternVersion = 1
)

type FilePreparationReport struct {
	ConnectionAttempts    uint64    `json:"connection_attempts"`
	PartCalls             uint64    `json:"part_calls"`
	PartResponsesOK       uint64    `json:"part_responses_ok"`
	AcknowledgedPartBytes uint64    `json:"acknowledged_part_bytes"`
	AssembleCalls         uint64    `json:"assemble_calls"`
	Enabled               bool      `json:"enabled"`
	StartedAt             time.Time `json:"started_at"`
	FinishedAt            time.Time `json:"finished_at"`
	Stage                 string    `json:"stage"`
	Disposition           string    `json:"disposition,omitempty"`
	ErrorClass            string    `json:"error_class,omitempty"`
}

// persistedFileFixture keeps only the stable location of a synthetic load-test
// document. It contains no auth key or login secret and is owner-readable so a
// test bundle can reuse the same server-side file across independent runs.
type persistedFileFixture struct {
	Version        int       `json:"version"`
	CreatedAt      time.Time `json:"created_at"`
	ServerAddress  string    `json:"server_address"`
	DC             int       `json:"dc"`
	SizeBytes      int       `json:"size_bytes"`
	PatternVersion int       `json:"pattern_version"`
	DocumentID     int64     `json:"document_id"`
	AccessHash     int64     `json:"access_hash"`
	FileReference  []byte    `json:"file_reference"`
}

func (f *persistedFileFixture) validate(endpoint Endpoint, size int) error {
	if f == nil {
		return errors.New("nil file fixture")
	}
	if f.Version != fileFixtureVersion || f.PatternVersion != fixturePatternVersion {
		return errors.New("file fixture version does not match the harness")
	}
	if f.ServerAddress != endpoint.Address || f.DC != endpoint.DC {
		return errors.New("file fixture endpoint does not match the manifest")
	}
	if f.SizeBytes != size || f.SizeBytes <= 0 {
		return fmt.Errorf("file fixture size %d does not match requested %d", f.SizeBytes, size)
	}
	if f.DocumentID <= 0 || f.AccessHash == 0 || len(f.FileReference) == 0 || len(f.FileReference) > 4096 {
		return errors.New("file fixture has an incomplete document location")
	}
	return nil
}

func (f *persistedFileFixture) runtime(chunk int) *downloadFixture {
	return &downloadFixture{
		location: &tg.InputDocumentFileLocation{
			ID: f.DocumentID, AccessHash: f.AccessHash,
			FileReference: append([]byte(nil), f.FileReference...),
		},
		size: f.SizeBytes, chunk: chunk,
	}
}

func persistedFixture(endpoint Endpoint, fixture *downloadFixture) *persistedFileFixture {
	return &persistedFileFixture{
		Version: fileFixtureVersion, CreatedAt: time.Now().UTC(),
		ServerAddress: endpoint.Address, DC: endpoint.DC,
		SizeBytes: fixture.size, PatternVersion: fixturePatternVersion,
		DocumentID: fixture.location.ID, AccessHash: fixture.location.AccessHash,
		FileReference: append([]byte(nil), fixture.location.FileReference...),
	}
}

func resolveFileFixturePath(manifestPath, configured string) string {
	configured = strings.TrimSpace(configured)
	if configured == "" {
		return filepath.Join(filepath.Dir(manifestPath), "file-fixture.json")
	}
	if filepath.IsAbs(configured) {
		return configured
	}
	return filepath.Join(filepath.Dir(manifestPath), filepath.FromSlash(configured))
}

func loadPersistedFileFixture(path string, endpoint Endpoint, size, chunk int) (*downloadFixture, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() > 64<<10 {
		return nil, errors.New("file fixture must be a private bounded regular file")
	}
	data, err := io.ReadAll(io.LimitReader(f, 64<<10+1))
	if err != nil || len(data) > 64<<10 || !unchangedEnvironmentFile(path, st) {
		return nil, errors.New("file fixture read failed or source changed")
	}
	var fixture persistedFileFixture
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fixture); err != nil {
		return nil, fmt.Errorf("decode file fixture: %w", err)
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return nil, errors.New("file fixture has trailing data")
	}
	if err := fixture.validate(endpoint, size); err != nil {
		return nil, err
	}
	return fixture.runtime(chunk), nil
}

func writePersistedFileFixture(path string, endpoint Endpoint, fixture *downloadFixture) error {
	persisted := persistedFixture(endpoint, fixture)
	if err := persisted.validate(endpoint, fixture.size); err != nil {
		return err
	}
	data, err := json.MarshalIndent(persisted, "", "  ")
	if err != nil {
		return fmt.Errorf("encode file fixture: %w", err)
	}
	return writeNewEvidenceReport(path, append(data, '\n'))
}
