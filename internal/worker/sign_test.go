package worker

import "testing"

func TestSignMatchesReferenceVector(t *testing.T) {
	// printf '%s' '1767225600.{"id":"e1"}' | openssl dgst -sha256 -hmac whsec_test
	want := "v1=454f7f9597bf74f9ee7b13eab0ac98e91665e9039d783864ee4129a0f2c9c88f"
	if got := Sign("whsec_test", 1767225600, []byte(`{"id":"e1"}`)); got != want {
		t.Fatalf("Sign = %s, want %s", got, want)
	}
}

func TestSignDependsOnSecretTimestampAndBody(t *testing.T) {
	base := Sign("s", 1, []byte("b"))
	for name, other := range map[string]string{
		"secret":    Sign("t", 1, []byte("b")),
		"timestamp": Sign("s", 2, []byte("b")),
		"body":      Sign("s", 1, []byte("c")),
	} {
		if other == base {
			t.Errorf("changing the %s did not change the signature", name)
		}
	}
}
