package store

import "testing"

func TestUUIDv5KnownVector(t *testing.T) {
	// RFC 9562 appendix A.4: UUIDv5 of "www.example.com" in the DNS namespace.
	dns := MustParseUUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	got := NewUUIDv5(dns, []byte("www.example.com")).String()
	if want := "2ed6657d-e927-568b-95e1-2665a8aea6a2"; got != want {
		t.Fatalf("uuidv5 = %s, want %s", got, want)
	}
}

func TestDeliveryIDDeterministicAndDistinct(t *testing.T) {
	w1 := MustParseUUID("00000000-0000-4000-8000-000000000001")
	w2 := MustParseUUID("00000000-0000-4000-8000-000000000002")

	if DeliveryID("evt-1", w1) != DeliveryID("evt-1", w1) {
		t.Fatal("same inputs gave different IDs")
	}
	ids := map[UUID]string{}
	for _, c := range []struct {
		event string
		hook  UUID
	}{{"evt-1", w1}, {"evt-1", w2}, {"evt-2", w1}, {"evt-", w1}, {"", w1}} {
		id := DeliveryID(c.event, c.hook)
		if prev, dup := ids[id]; dup {
			t.Fatalf("collision between %s and %s/%s", prev, c.event, c.hook)
		}
		ids[id] = c.event + "/" + c.hook.String()
	}
}

func TestParseUUIDRoundTripAndErrors(t *testing.T) {
	u := NewRandomUUID()
	back, err := ParseUUID(u.String())
	if err != nil || back != u {
		t.Fatalf("round trip %s -> %s, %v", u, back, err)
	}
	if u[6]>>4 != 4 || u[8]>>6 != 2 {
		t.Fatalf("random uuid %s has wrong version/variant", u)
	}
	for _, bad := range []string{"", "not-a-uuid", "6ba7b8109dad11d180b400c04fd430c8", "6ba7b810-9dad-11d1-80b4-00c04fd430cZ"} {
		if _, err := ParseUUID(bad); err == nil {
			t.Errorf("ParseUUID(%q) succeeded", bad)
		}
	}
}
