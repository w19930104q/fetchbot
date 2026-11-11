// Copyright 2014 Martin Angers and Contributors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fetchbot

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingDoer records the paths it was asked to fetch and optionally fails or
// blocks. It makes the tests independent from a real network/server.
type countingDoer struct {
	mu       sync.Mutex
	paths    []string
	failPath string
	gate     func(*http.Request)
}

func (d *countingDoer) Do(req *http.Request) (*http.Response, error) {
	d.mu.Lock()
	d.paths = append(d.paths, req.URL.Path)
	d.mu.Unlock()

	if d.gate != nil {
		d.gate(req)
	}
	if d.failPath != "" && req.URL.Path == d.failPath {
		return nil, errors.New("doer: request failed")
	}
	return &http.Response{Request: req, StatusCode: http.StatusOK}, nil
}

func (d *countingDoer) requestedPaths() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.paths...)
}

func (d *countingDoer) requestsForPath(path string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	cnt := 0
	for _, p := range d.paths {
		if p == path {
			cnt++
		}
	}
	return cnt
}

// gateHandler records every handled URL. When release is non-nil, it blocks
// until that channel is closed; when entered is non-nil, it signals each entry.
type gateHandler struct {
	mu      sync.Mutex
	handled []string
	errs    map[string]error
	entered chan string
	release <-chan struct{}
	fn      func(*Context, *http.Response, error)
}

func newGateHandler() *gateHandler {
	return &gateHandler{errs: make(map[string]error)}
}

func (h *gateHandler) Handle(ctx *Context, res *http.Response, err error) {
	rawurl := ctx.Cmd.URL().String()
	h.mu.Lock()
	h.handled = append(h.handled, rawurl)
	h.errs[rawurl] = err
	h.mu.Unlock()

	if h.entered != nil {
		h.entered <- rawurl
	}
	if h.release != nil {
		<-h.release
	}
	if h.fn != nil {
		h.fn(ctx, res, err)
	}
}

func (h *gateHandler) handledURLs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.handled...)
}

func (h *gateHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.handled)
}

func (h *gateHandler) errorFor(rawurl string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.errs[rawurl]
}

func pendingCount(q *Queue) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pending
}

func waitPending(t *testing.T, q *Queue, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if got := pendingCount(q); got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected %d pending commands, got %d", want, pendingCount(q))
		}
		time.Sleep(time.Millisecond)
	}
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func waitEntered(t *testing.T, ch <-chan string, what string) string {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return ""
	}
}

// TestMaxPendingTotalCap verifies that the cap is global across hosts, that a
// rejected command is neither requested nor handled, and that a slot is
// reclaimed once the Handler returns.
func TestMaxPendingTotalCap(t *testing.T) {
	const maxPending = 2
	release := make(chan struct{})
	entered := make(chan string, 16)
	h := newGateHandler()
	h.entered = entered
	h.release = release

	doer := &countingDoer{}
	f := New(h)
	f.HttpClient = doer
	f.MaxPending = maxPending
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	q := f.Start()

	urls := []string{
		"http://a.example/one",
		"http://b.example/two", // a different host proves the limit is global
		"http://c.example/three",
	}
	for i := 0; i < maxPending; i++ {
		if _, err := q.SendStringGet(urls[i]); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	// Give the host workers time to start their requests; the cap has already
	// been taken at admission time, so this is not needed for correctness, but
	// it makes the "rejected is never requested" assertion meaningful.
	for i := 0; i < maxPending; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for handler %d", i)
		}
	}

	if n, err := q.SendStringGet(urls[2]); n != 0 || err != ErrQueueFull {
		t.Fatalf("expected (0, ErrQueueFull), got (%d, %v)", n, err)
	}
	if got := pendingCount(q); got != maxPending {
		t.Fatalf("expected %d pending, got %d", maxPending, got)
	}
	if c := doer.requestsForPath("/three"); c != 0 {
		t.Fatalf("rejected command was requested %d time(s)", c)
	}
	for _, u := range h.handledURLs() {
		if u == urls[2] {
			t.Fatalf("rejected command %q was passed to the Handler", u)
		}
	}

	close(release)
	waitPending(t, q, 0)

	// A slot was returned: the same queue must accept again.
	more := "http://d.example/four"
	if _, err := q.SendStringGet(more); err != nil {
		t.Fatalf("send after slot release: %v", err)
	}
	waitPending(t, q, 0)
	if c := doer.requestsForPath("/four"); c != 1 {
		t.Fatalf("expected /four to be requested once, got %d", c)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if got := h.count(); got != maxPending+1 {
		t.Fatalf("expected %d handled commands, got %d", maxPending+1, got)
	}
}

// TestMaxPendingSendStringPrefix verifies that SendString* reports the admitted
// prefix length and the first error when the cap is reached.
func TestMaxPendingSendStringPrefix(t *testing.T) {
	const maxPending = 2
	release := make(chan struct{})
	h := newGateHandler()
	h.release = release

	f := New(h)
	f.HttpClient = &countingDoer{}
	f.MaxPending = maxPending
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	q := f.Start()

	urls := []string{
		"http://example.com/a",
		"http://example.com/b",
		"http://example.com/c",
		"http://example.com/d",
	}
	n, err := q.SendStringGet(urls...)
	if n != maxPending {
		t.Fatalf("expected %d admitted URLs, got %d", maxPending, n)
	}
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
	if got := pendingCount(q); got != maxPending {
		t.Fatalf("expected %d pending, got %d", maxPending, got)
	}
	close(release)
	waitPending(t, q, 0)
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	handled := h.handledURLs()
	for _, u := range urls[maxPending:] {
		for _, got := range handled {
			if got == u {
				t.Fatalf("rejected URL %q was handled", u)
			}
		}
	}
	if len(handled) != maxPending {
		t.Fatalf("expected %d handled commands, got %d", maxPending, len(handled))
	}
}

// TestMaxPendingCallbackSend verifies that Send from inside a Handler neither
// deadlocks nor bypasses the cap: with a free slot it is admitted, and at the
// cap it returns ErrQueueFull until the current Handler returns.
func TestMaxPendingCallbackSend(t *testing.T) {
	t.Run("slot available", func(t *testing.T) {
		result := make(chan struct {
			n   int
			err error
		}, 1)
		handledB := make(chan struct{}, 1)
		h := newGateHandler()
		var once sync.Once
		h.fn = func(ctx *Context, res *http.Response, err error) {
			if ctx.Cmd.URL().Path == "/a" {
				once.Do(func() {
					n, err := ctx.Q.SendStringGet("http://example.com/b")
					result <- struct {
						n   int
						err error
					}{n, err}
				})
			}
			if ctx.Cmd.URL().Path == "/b" {
				handledB <- struct{}{}
			}
		}
		f := New(h)
		f.HttpClient = &countingDoer{}
		f.MaxPending = 2
		f.DisablePoliteness = true
		f.CrawlDelay = 0
		q := f.Start()

		if _, err := q.SendStringGet("http://example.com/a"); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-result:
			if got.n != 1 || got.err != nil {
				t.Fatalf("callback Send: expected (1, nil), got (%d, %v)", got.n, got.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Send from Handler deadlocked or never ran")
		}
		waitClosed(t, handledB, "Handler for /b")
		waitPending(t, q, 0)
		if err := q.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("at cap", func(t *testing.T) {
		// With MaxPending == 1, the running command still owns its slot while
		// its Handler executes, so an in-Handler Send must fail fast rather
		// than wait for the slot (which would deadlock).
		result := make(chan error, 1)
		h := newGateHandler()
		h.fn = func(ctx *Context, res *http.Response, err error) {
			if ctx.Cmd.URL().Path == "/a" {
				_, sendErr := ctx.Q.SendStringGet("http://example.com/b")
				result <- sendErr
			}
		}
		f := New(h)
		f.HttpClient = &countingDoer{}
		f.MaxPending = 1
		f.DisablePoliteness = true
		f.CrawlDelay = 0
		q := f.Start()

		if _, err := q.SendStringGet("http://example.com/a"); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-result:
			if err != ErrQueueFull {
				t.Fatalf("expected ErrQueueFull from callback Send, got %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Send from Handler at cap deadlocked")
		}
		waitPending(t, q, 0)
		if err := q.Close(); err != nil {
			t.Fatal(err)
		}
		if c := h.count(); c != 1 {
			t.Fatalf("expected only the original command to be handled, got %d", c)
		}
	})
}

// TestMaxPendingQuotaReturned verifies that a slot is released after the
// Handler returns for request errors and robots.txt rejections.
func TestMaxPendingQuotaReturned(t *testing.T) {
	t.Run("request error", func(t *testing.T) {
		release := make(chan struct{})
		entered := make(chan string, 2)
		h := newGateHandler()
		h.entered = entered
		h.release = release

		doer := &countingDoer{failPath: "/fail"}
		f := New(h)
		f.HttpClient = doer
		f.MaxPending = 1
		f.DisablePoliteness = true
		f.CrawlDelay = 0
		q := f.Start()

		if _, err := q.SendStringGet("http://example.com/fail"); err != nil {
			t.Fatalf("send /fail: %v", err)
		}
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for /fail Handler")
		}
		close(release)
		waitPending(t, q, 0)
		if err := h.errorFor("http://example.com/fail"); err == nil {
			t.Fatal("expected the Handler to receive the request error")
		}

		if _, err := q.SendStringGet("http://example.com/ok"); err != nil {
			t.Fatalf("send after failed request: %v", err)
		}
		waitPending(t, q, 0)
		if err := q.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("robots disallowed", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/robots.txt" {
				w.Write([]byte("User-agent: *\nDisallow: /a\n"))
				return
			}
			w.Write([]byte("ok"))
		}))
		defer srv.Close()

		release := make(chan struct{})
		entered := make(chan string, 2)
		h := newGateHandler()
		h.entered = entered
		h.release = release

		f := New(h)
		f.MaxPending = 1
		f.CrawlDelay = 0
		q := f.Start()

		if _, err := q.SendStringGet(srv.URL + "/a"); err != nil {
			t.Fatalf("send /a: %v", err)
		}
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for disallowed Handler")
		}
		close(release)
		waitPending(t, q, 0)
		if err := h.errorFor(srv.URL + "/a"); err != ErrDisallowed {
			t.Fatalf("expected ErrDisallowed, got %v", err)
		}

		if _, err := q.SendStringGet(srv.URL + "/b"); err != nil {
			t.Fatalf("send after robots rejection: %v", err)
		}
		waitPending(t, q, 0)
		if err := q.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

// TestMaxPendingRobotsNotCounted verifies that internal robots.txt requests do
// not consume MaxPending slots.
func TestMaxPendingRobotsNotCounted(t *testing.T) {
	robotsStarted := make(chan struct{}, 4)
	releaseRobots := make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			once.Do(func() { close(robotsStarted) })
			<-releaseRobots
			w.Write([]byte("User-agent: *\nAllow: /\n"))
			return
		}
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	h := newGateHandler()
	f := New(h)
	f.MaxPending = 2
	f.CrawlDelay = 0
	q := f.Start()

	// The first business command holds one slot while its host's robots.txt
	// request is in flight.
	if _, err := q.SendStringGet(srv.URL + "/one"); err != nil {
		t.Fatalf("send /one: %v", err)
	}
	waitClosed(t, robotsStarted, "robots.txt request")

	// One business command is pending. If the internal robots request also
	// consumed a slot, pending would be 2 and this Send would be rejected.
	if _, err := q.SendStringGet(srv.URL + "/two"); err != nil {
		t.Fatalf("robots.txt request must not consume a MaxPending slot: %v", err)
	}
	if got := pendingCount(q); got != 2 {
		t.Fatalf("expected 2 pending business commands, got %d", got)
	}

	close(releaseRobots)
	waitPending(t, q, 0)
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if c := h.count(); c != 2 {
		t.Fatalf("expected 2 handled commands, got %d", c)
	}
}

// TestMaxPendingCloseDrainsAdmitted verifies that Close waits for every
// admitted command's Handler to return, even when the cap was reached.
func TestMaxPendingCloseDrainsAdmitted(t *testing.T) {
	const maxPending = 3
	release := make(chan struct{})
	entered := make(chan string, maxPending*2)
	h := newGateHandler()
	h.entered = entered
	h.release = release

	f := New(h)
	f.HttpClient = &countingDoer{}
	f.MaxPending = maxPending
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	q := f.Start()

	urls := []string{
		"http://example.com/1",
		"http://example.com/2",
		"http://example.com/3",
	}
	for _, u := range urls {
		if _, err := q.SendStringGet(u); err != nil {
			t.Fatalf("send %q: %v", u, err)
		}
	}
	if got := pendingCount(q); got != maxPending {
		t.Fatalf("expected %d pending, got %d", maxPending, got)
	}
	// One host worker processes commands sequentially, so only the first
	// Handler can be running while the other admitted commands are queued.
	waitEntered(t, entered, "first Handler entry")

	closeResults := make(chan error, 1)
	go func() { closeResults <- q.Close() }()

	// Close must not return while Handlers are still running.
	select {
	case err := <-closeResults:
		t.Fatalf("Close returned before admitted Handlers completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-closeResults:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after Handlers completed")
	}

	waitPending(t, q, 0)
	select {
	case <-q.Done():
	default:
		t.Fatal("Done must be closed after Close returns")
	}
	if got := h.count(); got != maxPending {
		t.Fatalf("expected %d handled commands, got %d", maxPending, got)
	}
	// Closed queue rejects new commands.
	if _, err := q.SendStringGet("http://example.com/4"); err != ErrQueueClosed {
		t.Fatalf("expected ErrQueueClosed, got %v", err)
	}
}

// TestMaxPendingCancelReleasesSlots verifies that Cancel keeps its discard
// semantics while releasing the slots of every admitted command.
func TestMaxPendingCancelReleasesSlots(t *testing.T) {
	const maxPending = 64

	entered := make(chan string, maxPending)
	releaseFirst := make(chan struct{})
	h := newGateHandler()
	h.entered = entered
	h.release = releaseFirst

	f := New(h)
	f.HttpClient = &countingDoer{}
	f.MaxPending = maxPending
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	q := f.Start()

	// Fill the cap; all Handlers block, so no slot is released.
	for i := 0; i < maxPending; i++ {
		if _, err := q.SendStringGet(fmt.Sprintf("http://example.com/%d", i)); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	if got := pendingCount(q); got != maxPending {
		t.Fatalf("expected %d pending, got %d", maxPending, got)
	}
	// Wait for the first Handler (the single host worker is now blocked).
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first Handler")
	}

	cancelResults := make(chan error, 1)
	go func() { cancelResults <- q.Cancel() }()

	// Cancel cannot complete while the first Handler is still running.
	select {
	case err := <-cancelResults:
		t.Fatalf("Cancel returned while a Handler was still running: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFirst)
	select {
	case err := <-cancelResults:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Cancel did not return")
	}

	waitPending(t, q, 0)
	select {
	case <-q.Done():
	default:
		t.Fatal("Done must be closed after Cancel returns")
	}
	// Only the command whose Handler was already running may have completed.
	if got := h.count(); got > 1 {
		t.Fatalf("Cancel processed %d commands, expected at most 1", got)
	}
}

// TestMaxPendingConcurrentSendCloseCancel runs Send, Close and Cancel
// concurrently to exercise the admission/close race. Run this with -race.
func TestMaxPendingConcurrentSendCloseCancel(t *testing.T) {
	h := newGateHandler()
	f := New(h)
	f.HttpClient = &countingDoer{}
	f.MaxPending = 8
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	q := f.Start()

	const senders = 8
	const perSender = 200
	var wg sync.WaitGroup
	var success int64
	var rejected int64
	errs := make(chan error, senders)

	wg.Add(senders)
	for s := 0; s < senders; s++ {
		go func(s int) {
			defer wg.Done()
			for i := 0; i < perSender; i++ {
				rawurl := fmt.Sprintf("http://example.com/%d/%d", s, i)
				_, err := q.SendStringGet(rawurl)
				switch err {
				case nil:
					atomic.AddInt64(&success, 1)
				case ErrQueueFull:
					atomic.AddInt64(&rejected, 1)
				case ErrQueueClosed:
					return
				default:
					select {
					case errs <- err:
					default:
					}
					return
				}
			}
		}(s)
	}

	// Race a Close and a Cancel against the senders.
	var closeWg sync.WaitGroup
	closeWg.Add(2)
	closeErr := make(chan error, 2)
	go func() {
		defer closeWg.Done()
		closeErr <- q.Close()
	}()
	go func() {
		defer closeWg.Done()
		closeErr <- q.Cancel()
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		closeWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent Send/Close/Cancel did not complete")
	}
	close(errs)
	for err := range errs {
		t.Fatalf("unexpected Send error: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := <-closeErr; err != nil {
			t.Fatalf("Close/Cancel error: %v", err)
		}
	}

	if got := pendingCount(q); got != 0 {
		t.Fatalf("expected no pending commands after shutdown, got %d", got)
	}
	select {
	case <-q.Done():
	default:
		t.Fatal("Done must be closed")
	}
	if _, err := q.SendStringGet("http://example.com/late"); err != ErrQueueClosed {
		t.Fatalf("expected ErrQueueClosed after shutdown, got %v", err)
	}

	// Admitted commands are either processed or discarded by Cancel; either
	// way the number of Handler calls can never exceed successful admissions.
	handled := int64(h.count())
	if handled > success {
		t.Fatalf("handled %d commands but only %d were admitted", handled, success)
	}
	t.Logf("admitted=%d handled=%d rejected(full)=%d", success, handled, rejected)
}

// TestMaxPendingZeroUnlimited verifies that the default (zero) keeps the
// original unlimited behavior.
func TestMaxPendingZeroUnlimited(t *testing.T) {
	const total = 50
	release := make(chan struct{})
	h := newGateHandler()
	h.release = release

	f := New(h)
	f.HttpClient = &countingDoer{}
	f.CrawlDelay = 0
	f.DisablePoliteness = true
	q := f.Start()

	for i := 0; i < total; i++ {
		if _, err := q.SendStringGet(fmt.Sprintf("http://example.com/%d", i)); err != nil {
			t.Fatalf("send %d with MaxPending=0: %v", i, err)
		}
	}
	if got := pendingCount(q); got != total {
		t.Fatalf("expected %d pending, got %d", total, got)
	}
	close(release)
	waitPending(t, q, 0)
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if got := h.count(); got != total {
		t.Fatalf("expected %d handled commands, got %d", total, got)
	}
}

// TestMaxPendingRestart verifies that sequential Start calls each get an
// independent, correctly configured quota.
func TestMaxPendingRestart(t *testing.T) {
	release := make(chan struct{})
	h := newGateHandler()
	h.release = release
	doer := &countingDoer{}

	f := New(h)
	f.HttpClient = doer
	f.DisablePoliteness = true
	f.CrawlDelay = 0

	// First run: MaxPending = 1.
	f.MaxPending = 1
	q1 := f.Start()
	if _, err := q1.SendStringGet("http://example.com/run1-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := q1.SendStringGet("http://example.com/run1-b"); err != ErrQueueFull {
		t.Fatalf("run 1: expected ErrQueueFull, got %v", err)
	}
	close(release)
	waitPending(t, q1, 0)
	if err := q1.Close(); err != nil {
		t.Fatal(err)
	}
	release = make(chan struct{})
	h.release = release

	// Second run: a fresh queue with MaxPending = 2.
	f.MaxPending = 2
	q2 := f.Start()
	if _, err := q2.SendStringGet("http://example.com/run2-a", "http://example.com/run2-b"); err != nil {
		t.Fatalf("run 2: %v", err)
	}
	if _, err := q2.SendStringGet("http://example.com/run2-c"); err != ErrQueueFull {
		t.Fatalf("run 2: expected ErrQueueFull, got %v", err)
	}
	close(release)
	waitPending(t, q2, 0)
	if err := q2.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestMaxPendingNoGoroutineLeak checks that shutdown releases all goroutines
// even when the cap was constantly reached.
func TestMaxPendingNoGoroutineLeak(t *testing.T) {
	startGoros := runtime.NumGoroutine()

	h := newGateHandler()
	f := New(h)
	f.MaxPending = 4
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	f.HttpClient = &countingDoer{}
	q := f.Start()

	var wg sync.WaitGroup
	var senders = 4
	wg.Add(senders)
	for s := 0; s < senders; s++ {
		go func(s int) {
			defer wg.Done()
			for i := 0; ; i++ {
				_, err := q.SendStringGet(fmt.Sprintf("http://example.com/%d/%d", s, i))
				if err == ErrQueueClosed {
					return
				}
				if err != nil && err != ErrQueueFull {
					t.Errorf("unexpected Send error: %v", err)
					return
				}
			}
		}(s)
	}

	time.Sleep(50 * time.Millisecond)
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	// The queue's goroutines are joined by Close; allow transient runtime
	// goroutines to disappear.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if runtime.NumGoroutine() <= startGoros+1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine leak: started with %d, now %d", startGoros, runtime.NumGoroutine())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
