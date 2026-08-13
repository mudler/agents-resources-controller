// Package logstore keeps one append-only file per job and lets clients follow
// it while the job is still running.
package logstore

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

var safeID = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

type Store struct {
	dir string

	mu        sync.Mutex
	listeners map[string][]chan struct{}
}

func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create log dir: %w", err)
	}
	return &Store{dir: dir, listeners: map[string][]chan struct{}{}}, nil
}

func (s *Store) path(jobID string) (string, error) {
	if !safeID.MatchString(jobID) {
		return "", fmt.Errorf("invalid job id %q", jobID)
	}
	return filepath.Join(s.dir, jobID+".log"), nil
}

func (s *Store) Append(jobID string, chunk []byte) error {
	p, err := s.path(jobID)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(chunk); err != nil {
		return err
	}
	s.wake(jobID)
	return nil
}

func (s *Store) wake(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ch := range s.listeners[jobID] {
		select {
		case ch <- struct{}{}:
		default: // a pending wakeup is as good as another
		}
	}
}

func (s *Store) subscribe(jobID string) (chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	s.mu.Lock()
	s.listeners[jobID] = append(s.listeners[jobID], ch)
	s.mu.Unlock()

	return ch, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		out := s.listeners[jobID][:0]
		for _, c := range s.listeners[jobID] {
			if c != ch {
				out = append(out, c)
			}
		}
		s.listeners[jobID] = out
	}
}

func (s *Store) Read(jobID string) ([]byte, error) {
	p, err := s.path(jobID)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return b, err
}

// Follow streams the file from the beginning and then tails it. The returned
// channel closes when done closes, the context ends, or the file is unreadable.
func (s *Store) Follow(ctx context.Context, jobID string, done <-chan struct{}) (<-chan []byte, error) {
	p, err := s.path(jobID)
	if err != nil {
		return nil, err
	}
	// Create it so a follower attached before the first write does not fail.
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDONLY, 0o644)
	if err != nil {
		return nil, err
	}

	wake, unsubscribe := s.subscribe(jobID)
	out := make(chan []byte)

	go func() {
		defer close(out)
		defer f.Close()
		defer unsubscribe()

		buf := make([]byte, 32*1024)
		drain := func() bool {
			for {
				n, err := f.Read(buf)
				if n > 0 {
					chunk := make([]byte, n)
					copy(chunk, buf[:n])
					select {
					case out <- chunk:
					case <-ctx.Done():
						return false
					}
					continue
				}
				if err == io.EOF || err == nil {
					return true
				}
				return false
			}
		}

		for {
			if !drain() {
				return
			}
			select {
			case <-wake:
			case <-done:
				drain() // flush whatever landed just before the job ended
				return
			case <-ctx.Done():
				return
			case <-time.After(time.Second): // safety net against a missed wakeup
			}
		}
	}()

	return out, nil
}
