package job

import (
	"errors"
	"fmt"
	"testing"
)

func TestCanTransition(t *testing.T) {
	allowed := map[[2]Status]bool{
		{StatusPending, StatusProcessing}:    true,
		{StatusProcessing, StatusCompleted}:  true,
		{StatusProcessing, StatusPending}:    true,
		{StatusProcessing, StatusFailed}:     true,
		{StatusProcessing, StatusDeadLetter}: true,
		{StatusDeadLetter, StatusPending}:    true,
	}
	all := []Status{StatusPending, StatusProcessing, StatusCompleted, StatusFailed, StatusDeadLetter}

	for _, from := range all {
		for _, to := range all {
			want := allowed[[2]Status{from, to}]
			if got := CanTransition(from, to); got != want {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestTerminalStatusesHaveNoTransitions(t *testing.T) {
	for _, s := range []Status{StatusCompleted, StatusFailed} {
		if len(transitions[s]) != 0 {
			t.Errorf("%s should be terminal, has transitions %v", s, transitions[s])
		}
	}
}

func TestPermanentIsDetectedThroughWrapping(t *testing.T) {
	base := errors.New("bad payload")
	err := fmt.Errorf("handler: %w", Permanent(base))

	if !IsPermanent(err) {
		t.Fatal("expected wrapped permanent error to be detected")
	}
	if !errors.Is(err, base) {
		t.Fatal("expected permanent error to preserve the original error chain")
	}
	if IsPermanent(base) {
		t.Fatal("plain errors must be retryable")
	}
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) must be nil")
	}
}

func TestTransitionErrorMatchesSentinel(t *testing.T) {
	err := error(&TransitionError{From: StatusCompleted, To: StatusPending})
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatal("TransitionError must unwrap to ErrInvalidTransition")
	}
}
