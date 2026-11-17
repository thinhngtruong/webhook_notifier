package store

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
)

// UUID is a 16-byte UUID that pgx encodes to and decodes from Postgres uuid.
type UUID [16]byte

// deliveryNamespace is the UUIDv5 namespace for delivery IDs. Changing it
// would change every delivery ID and break deduplication of redelivered
// events, so it is fixed forever.
var deliveryNamespace = MustParseUUID("5b1f2a8e-6c2d-4f0b-9a57-3f1c8d2e7a40")

// DeliveryID is the deterministic ID of the delivery of eventID to webhookID:
// a UUIDv5 (RFC 9562) over "<event_id>\x00<webhook_id>". It is the dedup key
// in the deliveries table and the X-Webhook-Id header sent to partners.
func DeliveryID(eventID string, webhookID UUID) UUID {
	name := make([]byte, 0, len(eventID)+1+36)
	name = append(name, eventID...)
	name = append(name, 0)
	name = append(name, webhookID.String()...)
	return NewUUIDv5(deliveryNamespace, name)
}

// NewUUIDv5 returns the name-based (SHA-1) UUID of name in namespace.
func NewUUIDv5(namespace UUID, name []byte) UUID {
	h := sha1.New()
	h.Write(namespace[:])
	h.Write(name)
	var u UUID
	copy(u[:], h.Sum(nil))
	u[6] = (u[6] & 0x0f) | 0x50 // version 5
	u[8] = (u[8] & 0x3f) | 0x80 // RFC 9562 variant
	return u
}

// NewRandomUUID returns a random (version 4) UUID.
func NewRandomUUID() UUID {
	var u UUID
	if _, err := rand.Read(u[:]); err != nil {
		panic(fmt.Sprintf("store: crypto/rand failed: %v", err))
	}
	u[6] = (u[6] & 0x0f) | 0x40
	u[8] = (u[8] & 0x3f) | 0x80
	return u
}

// ParseUUID parses the canonical 8-4-4-4-12 hex form.
func ParseUUID(s string) (UUID, error) {
	var u UUID
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return u, fmt.Errorf("parse uuid %q: invalid format", s)
	}
	hexStr := s[0:8] + s[9:13] + s[14:18] + s[19:23] + s[24:36]
	if _, err := hex.Decode(u[:], []byte(hexStr)); err != nil {
		return u, fmt.Errorf("parse uuid %q: %w", s, err)
	}
	return u, nil
}

// MustParseUUID is ParseUUID that panics; for constants.
func MustParseUUID(s string) UUID {
	u, err := ParseUUID(s)
	if err != nil {
		panic(err)
	}
	return u
}

func (u UUID) String() string {
	var b [36]byte
	hex.Encode(b[0:8], u[0:4])
	b[8] = '-'
	hex.Encode(b[9:13], u[4:6])
	b[13] = '-'
	hex.Encode(b[14:18], u[6:8])
	b[18] = '-'
	hex.Encode(b[19:23], u[8:10])
	b[23] = '-'
	hex.Encode(b[24:36], u[10:16])
	return string(b[:])
}

// UUIDValue implements pgtype.UUIDValuer.
func (u UUID) UUIDValue() (pgtype.UUID, error) {
	return pgtype.UUID{Bytes: u, Valid: true}, nil
}

// ScanUUID implements pgtype.UUIDScanner. NULL scans to the zero UUID.
func (u *UUID) ScanUUID(v pgtype.UUID) error {
	if !v.Valid {
		*u = UUID{}
		return nil
	}
	*u = v.Bytes
	return nil
}
