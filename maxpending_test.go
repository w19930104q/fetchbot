// Copyright 2014 Martin Angers and Contributors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fetchbot

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// tokensUsed returns the number of pending slots currently held. It is 0 for
// an unlimited Queue.
func tokensUsed(q *Queue) int {
	if q.tokens == nil {
		return 0
	}
	return len(q.tokens)
}

// waitTokens waits until the Queue holds exactly want pending slots.
func waitTokens(t *testing.T, q *Queue, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for tokensUsed(q) != want {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d used tokens, got %d", want, tokensUsed(q))
		}
		time.Sleep(time.Millisecond)
	}
}

// TestMaxPendingLimit verifies that at most MaxPending commands are pending at
// any time (queued, in-flight or in their Handler), that a full Queue makes
// Send fail immediately with ErrQueueFull, that SendStringGet returns the
// admitted prefix length along with the first error, and that rejected
// commands are neither requested nor passed to the Handler.
func TestMaxPendingLimit(t *testing.T) {
	var reqCount int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&reqCount, 1)
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	unblock := make(chan struct{})
	started := make(chan struct{}, 10)
	var handled int64
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) {
		started <- struct{}{}
		<-unblock
		atomic.AddInt64(&handled, 1)
	})

	f := New(sh)
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	f.MaxPending = 3
	q := f.Start()

	// Admit 2 commands, one slot remains.
	if n, err := q.SendStringGet(srv.URL + "/0"); n != 1 || err != nil {
		t.Fatalf("expected (1, nil), got (%d, %v)", n, err)
	}
	if n, err := q.SendStringGet(srv.URL + "/1"); n != 1 || err != nil {
		t.Fatalf("expected (1, nil), got (%d, %v)", n, err)
	}
	// Wait for the first command to reach its (blocking) handler, all 2 slots
	// are held.
	<-started
	waitTokens(t, q, 2)

	// The variadic call admits only the first URL, then hits the limit.
	n, err := q.SendStringGet(srv.URL+"/2", srv.URL+"/3", srv.URL+"/4")
	if n != 1 || err != ErrQueueFull {
		t.Fatalf("expected (1, ErrQueueFull), got (%d, %v)", n, err)
	}
	waitTokens(t, q, 3)

	// A single Send on a full queue fails immediately with ErrQueueFull.
	if err := q.Send(&Cmd{U: mustParse(t, srv.URL+"/5"), M: "GET"}); err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}

	// Release the handlers, all admitted commands get processed.
	close(unblock)
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	if got := atomic.LoadInt64(&handled); got != 3 {
		t.Errorf("expected 3 handled commands, got %d", got)
	}
	// The rejected commands (/3, /4, /5) must never have been requested.
	if got := atomic.LoadInt64(&reqCount); got != 3 {
		t.Errorf("expected 3 requests, got %d", got)
	}
	if got := tokensUsed(q); got != 0 {
		t.Errorf("expected all tokens released, got %d", got)
	}
}

// TestMaxPendingEnqueueFromHandler verifies that a Handler can enqueue new
// commands from within its callback without deadlocking, and that with
// MaxPending=1 (its own slot still held) it gets ErrQueueFull instead.
func TestMaxPendingEnqueueFromHandler(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	t.Run("SlotAvailable", func(t *testing.T) {
		nextHandled := make(chan struct{}, 1)
		seedErr := make(chan error, 1)
		var mu sync.Mutex
		paths := make(map[string]int)
		sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) {
			mu.Lock()
			paths[res.Request.URL.Path]++
			mu.Unlock()
			if res.Request.URL.Path == "/seed" {
				_, err := ctx.Q.SendStringGet(srv.URL + "/next")
				seedErr <- err
			} else if res.Request.URL.Path == "/next" {
				nextHandled <- struct{}{}
			}
		})

		f := New(sh)
		f.DisablePoliteness = true
		f.CrawlDelay = 0
		f.MaxPending = 2
		q := f.Start()
		if _, err := q.SendStringGet(srv.URL + "/seed"); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-seedErr:
			if err != nil {
				t.Errorf("expected seed handler Send to succeed, got %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("deadlock: seed handler did not return from Send")
		}
		select {
		case <-nextHandled:
		case <-time.After(5 * time.Second):
			t.Fatal("deadlock: /next was never handled")
		}
		q.Close()
		mu.Lock()
		defer mu.Unlock()
		if paths["/seed"] != 1 || paths["/next"] != 1 {
			t.Errorf("expected /seed and /next handled once, got %v", paths)
		}
		if got := tokensUsed(q); got != 0 {
			t.Errorf("expected all tokens released, got %d", got)
		}
	})

	t.Run("QueueFullFromHandler", func(t *testing.T) {
		seedErr := make(chan error, 1)
		sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) {
			// The only slot is held by this very command until the
			// handler returns, so the enqueue must fail immediately.
			_, err = ctx.Q.SendStringGet(srv.URL + "/next")
			seedErr <- err
		})

		f := New(sh)
		f.DisablePoliteness = true
		f.CrawlDelay = 0
		f.MaxPending = 1
		q := f.Start()
		if _, err := q.SendStringGet(srv.URL + "/seed"); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-seedErr:
			if err != ErrQueueFull {
				t.Errorf("expected ErrQueueFull, got %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("deadlock: seed handler did not return from Send")
		}
		q.Close()
		if got := tokensUsed(q); got != 0 {
			t.Errorf("expected all tokens released, got %d", got)
		}
	})
}

// TestMaxPendingCancelReleases verifies that Cancel discards pending commands
// and releases their slots, that the in-flight command's slot is released once
// its Handler returns, and that Done is closed only after all cleanup.
func TestMaxPendingCancelReleases(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	unblock := make(chan struct{})
	started := make(chan struct{}, 10)
	var handled int64
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) {
		started <- struct{}{}
		<-unblock
		atomic.AddInt64(&handled, 1)
	})

	f := New(sh)
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	f.MaxPending = 4
	q := f.Start()

	// Fill all 4 slots: 1 in the blocking handler, 3 queued.
	for i := 0; i < 4; i++ {
		if _, err := q.SendStringGet(srv.URL + "/" + strconv.Itoa(i)); err != nil {
			t.Fatal(err)
		}
	}
	<-started
	waitTokens(t, q, 4)

	// Cancel while the first handler is still blocked: it cannot complete
	// until the handler returns and the queued commands are drained.
	cancelDone := make(chan struct{})
	go func() {
		q.Cancel()
		close(cancelDone)
	}()
	// Wait for the cancelled signal to be visible, so that no further
	// command gets processed once the in-flight handler returns.
	<-q.cancelled
	// Let the in-flight handler finish so Cancel can complete.
	close(unblock)
	select {
	case <-cancelDone:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: Cancel did not return")
	}

	// Done must be closed once Cancel has returned.
	select {
	case <-q.Done():
	default:
		t.Error("expected Done to be closed after Cancel")
	}
	// Only the in-flight command was handled, the 3 queued ones discarded.
	if got := atomic.LoadInt64(&handled); got != 1 {
		t.Errorf("expected 1 handled command, got %d", got)
	}
	// All slots, including those of the discarded commands, are released.
	if got := tokensUsed(q); got != 0 {
		t.Errorf("expected all tokens released, got %d", got)
	}
	// The queue is closed: further sends are rejected.
	if err := q.Send(&Cmd{U: mustParse(t, srv.URL+"/x"), M: "GET"}); err != ErrQueueClosed {
		t.Errorf("expected ErrQueueClosed, got %v", err)
	}
}

// TestMaxPendingCloseInterleave hammers the Queue with concurrent Senders
// while Close (twice, concurrently) is called, and verifies that every
// admitted command is eventually handled (Close drains), all slots are
// released and no goroutine leaks.
func TestMaxPendingCloseInterleave(t *testing.T) {
	var admitted, handled int64
	f := New(HandlerFunc(func(ctx *Context, res *http.Response, err error) {
		atomic.AddInt64(&handled, 1)
	}))
	f.HttpClient = doerFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{Request: req, StatusCode: 200}, nil
	})
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	f.MaxPending = 8

	startGoros := runtime.NumGoroutine()
	q := f.Start()

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				_, err := q.SendStringGet(fmt.Sprintf("http://example-%d.com/%d", g, i))
				if err != nil {
					return
				}
				atomic.AddInt64(&admitted, 1)
			}
		}(g)
	}

	time.Sleep(10 * time.Millisecond)
	// Two concurrent Closes: both must return after the drain.
	closeDone := make(chan struct{}, 2)
	go func() { q.Close(); closeDone <- struct{}{} }()
	go func() { q.Close(); closeDone <- struct{}{} }()
	for i := 0; i < 2; i++ {
		select {
		case <-closeDone:
		case <-time.After(10 * time.Second):
			t.Fatal("deadlock: Close did not return")
		}
	}
	wg.Wait()

	// Close drains: every admitted command was handled exactly once.
	if a, h := atomic.LoadInt64(&admitted), atomic.LoadInt64(&handled); a != h {
		t.Errorf("expected all %d admitted commands handled, got %d", a, h)
	}
	if got := tokensUsed(q); got != 0 {
		t.Errorf("expected all tokens released, got %d", got)
	}

	time.Sleep(10 * time.Millisecond)
	if g := runtime.NumGoroutine(); startGoros < g {
		t.Errorf("want %d goroutines like there was at the start, got %d (leak)", startGoros, g)
	}
}

// TestMaxPendingCancelInterleave hammers the Queue with concurrent Senders
// while Cancel is called, and verifies that all slots are released, Done is
// closed and no goroutine leaks.
func TestMaxPendingCancelInterleave(t *testing.T) {
	var admitted, handled int64
	f := New(HandlerFunc(func(ctx *Context, res *http.Response, err error) {
		atomic.AddInt64(&handled, 1)
		time.Sleep(time.Millisecond)
	}))
	f.HttpClient = doerFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{Request: req, StatusCode: 200}, nil
	})
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	f.MaxPending = 8

	startGoros := runtime.NumGoroutine()
	q := f.Start()

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				_, err := q.SendStringGet(fmt.Sprintf("http://cancel-%d.com/%d", g, i))
				if err != nil {
					return
				}
				atomic.AddInt64(&admitted, 1)
			}
		}(g)
	}

	time.Sleep(10 * time.Millisecond)
	cancelDone := make(chan struct{})
	go func() { q.Cancel(); close(cancelDone) }()
	select {
	case <-cancelDone:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: Cancel did not return")
	}
	wg.Wait()

	select {
	case <-q.Done():
	default:
		t.Error("expected Done to be closed after Cancel")
	}
	// Cancel discards: handled can never exceed admitted.
	if a, h := atomic.LoadInt64(&admitted), atomic.LoadInt64(&handled); h > a {
		t.Errorf("expected handled (%d) <= admitted (%d)", h, a)
	}
	if got := tokensUsed(q); got != 0 {
		t.Errorf("expected all tokens released, got %d", got)
	}

	time.Sleep(10 * time.Millisecond)
	if g := runtime.NumGoroutine(); startGoros < g {
		t.Errorf("want %d goroutines like there was at the start, got %d (leak)", startGoros, g)
	}
}

// TestMaxPendingRobotsExcluded verifies that the internal robots.txt request
// does not consume a pending slot, and that a command rejected by the
// robots.txt policy releases its slot after the Handler call.
func TestMaxPendingRobotsExcluded(t *testing.T) {
	var mu sync.Mutex
	requests := make(map[string]int)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests[r.URL.Path]++
		mu.Unlock()
		if r.URL.Path == "/robots.txt" {
			w.Write([]byte("User-agent: *\nDisallow: /forbidden\n"))
			return
		}
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	type result struct {
		path string
		err  error
	}
	results := make(chan result, 2)
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) {
		results <- result{ctx.Cmd.URL().Path, err}
	})

	f := New(sh)
	f.CrawlDelay = 0
	f.MaxPending = 1
	q := f.Start()

	// With MaxPending=1, the internal robots.txt fetch must not take the
	// single slot away from the enqueued command.
	if _, err := q.SendStringGet(srv.URL + "/allowed"); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-results:
		if r.path != "/allowed" || r.err != nil {
			t.Errorf("expected /allowed with no error, got %s with %v", r.path, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: /allowed was never handled")
	}

	// A robots.txt-disallowed command is admitted, its Handler called with
	// ErrDisallowed, and its slot released afterwards. The slot of /allowed
	// is released after its Handler returned, wait for it.
	waitTokens(t, q, 0)
	if _, err := q.SendStringGet(srv.URL + "/forbidden"); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-results:
		if r.path != "/forbidden" || r.err != ErrDisallowed {
			t.Errorf("expected /forbidden with ErrDisallowed, got %s with %v", r.path, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: /forbidden was never handled")
	}
	q.Close()

	mu.Lock()
	defer mu.Unlock()
	if requests["/robots.txt"] != 1 {
		t.Errorf("expected 1 robots.txt request, got %d", requests["/robots.txt"])
	}
	if requests["/allowed"] != 1 {
		t.Errorf("expected 1 /allowed request, got %d", requests["/allowed"])
	}
	if requests["/forbidden"] != 0 {
		t.Errorf("expected no /forbidden request, got %d", requests["/forbidden"])
	}
	if got := tokensUsed(q); got != 0 {
		t.Errorf("expected all tokens released, got %d", got)
	}
}

// TestMaxPendingZeroUnlimited verifies that the default MaxPending value of 0
// preserves the original behavior: no Send ever fails with ErrQueueFull.
func TestMaxPendingZeroUnlimited(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	unblock := make(chan struct{})
	var handled int64
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) {
		<-unblock
		atomic.AddInt64(&handled, 1)
	})

	f := New(sh)
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	q := f.Start()

	const total = 20
	n, err := q.SendStringGet(func() []string {
		urls := make([]string, total)
		for i := range urls {
			urls[i] = srv.URL + "/" + strconv.Itoa(i)
		}
		return urls
	}()...)
	if n != total || err != nil {
		t.Fatalf("expected (%d, nil), got (%d, %v)", total, n, err)
	}
	close(unblock)
	q.Close()
	if got := atomic.LoadInt64(&handled); got != total {
		t.Errorf("expected %d handled commands, got %d", total, got)
	}
}

// TestMaxPendingRestart verifies that a Fetcher with MaxPending can be
// restarted after a Close, with a fresh budget of pending slots.
func TestMaxPendingRestart(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	var handled int64
	f := New(HandlerFunc(func(ctx *Context, res *http.Response, err error) {
		atomic.AddInt64(&handled, 1)
	}))
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	f.MaxPending = 2

	for run := 0; run < 2; run++ {
		q := f.Start()
		for i := 0; i < 5; i++ {
			// With MaxPending=2 a burst can hit the limit: wait for a
			// slot to be released and retry.
			for {
				_, err := q.SendStringGet(srv.URL + "/" + strconv.Itoa(i))
				if err == nil {
					break
				}
				if err != ErrQueueFull {
					t.Fatalf("run %d: %v", run, err)
				}
				time.Sleep(time.Millisecond)
			}
		}
		q.Close()
		if got := tokensUsed(q); got != 0 {
			t.Fatalf("run %d: expected all tokens released, got %d", run, got)
		}
	}
	if got := atomic.LoadInt64(&handled); got != 10 {
		t.Errorf("expected 10 handled commands, got %d", got)
	}
}
