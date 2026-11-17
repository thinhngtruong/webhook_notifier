package worker

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

// Header names sent with every delivery.
const (
	HeaderID        = "X-Webhook-Id"
	HeaderTimestamp = "X-Webhook-Timestamp"
	HeaderSignature = "X-Webhook-Signature"
)

// Sign returns the X-Webhook-Signature value for a body sent at unix
// timestamp ts: "v1=" + hex(HMAC-SHA256(secret, "<ts>.<body>")). Signing the
// timestamp lets receivers reject replays of old requests.
func Sign(secret string, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return "v1=" + hex.EncodeToString(mac.Sum(nil))
}
