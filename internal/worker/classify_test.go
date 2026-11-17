package worker

import (
	"context"
	"errors"
	"testing"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		status int
		err    error
		want   Outcome
	}{
		{200, nil, Succeeded},
		{201, nil, Succeeded},
		{204, nil, Succeeded},
		{299, nil, Succeeded},
		{408, nil, Retry},
		{429, nil, Retry},
		{500, nil, Retry},
		{502, nil, Retry},
		{503, nil, Retry},
		{599, nil, Retry},
		{410, nil, Gone},
		{400, nil, Dead},
		{401, nil, Dead},
		{403, nil, Dead},
		{404, nil, Dead},
		{422, nil, Dead},
		{301, nil, Dead}, // redirects are not followed: misconfigured URL
		{302, nil, Dead},
		{0, context.DeadlineExceeded, Retry},
		{0, errors.New("connection refused"), Retry},
	}
	for _, tt := range tests {
		if got := Classify(tt.status, tt.err); got != tt.want {
			t.Errorf("Classify(%d, %v) = %v, want %v", tt.status, tt.err, got, tt.want)
		}
	}
}
