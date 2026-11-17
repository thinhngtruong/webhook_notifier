package bench

import "testing"

// Regression: replicas must connect to the scenario's throwaway database,
// not to the admin database the URL started with.
func TestWithDatabaseReplacesTheName(t *testing.T) {
	got, err := withDatabase("postgres://u:p@localhost:55432/notifier?sslmode=disable", "bench_1")
	if err != nil {
		t.Fatal(err)
	}
	if want := "postgres://u:p@localhost:55432/bench_1?sslmode=disable"; got != want {
		t.Fatalf("withDatabase = %s, want %s", got, want)
	}
}
