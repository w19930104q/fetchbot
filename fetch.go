// Copyright 2014 Martin Angers and Contributors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fetchbot

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/temoto/robotstxt"
)

var (
	// ErrEmptyHost is returned if a command to be enqueued has an URL with an empty host.
	ErrEmptyHost = errors.New("fetchbot: invalid empty host")

	// ErrDisallowed is returned when the requested URL is disallowed by the robots.txt
	// policy.
	ErrDisallowed = errors.New("fetchbot: disallowed by robots.txt")

	// ErrQueueClosed is returned when a Send call is made on a closed Queue.
	ErrQueueClosed = errors.New("fetchbot: send on a closed queue")

	// ErrQueueFull is returned when a Send call is made on a Queue that has
	// reached its MaxPending limit. The rejected command is neither requested
	// nor passed to the Handler.
	ErrQueueFull = errors.New("fetchbot: queue is full")
)

// Parse the robots.txt relative path a single time at startup, this can't
// return an error.
var robotsTxtParsedPath, _ = url.Parse("/robots.txt")

const (
	// DefaultCrawlDelay represents the delay to use if there is no robots.txt
	// specified delay.
	DefaultCrawlDelay = 5 * time.Second

	// DefaultUserAgent is the default user agent string.
	DefaultUserAgent = "Fetchbot (https://github.com/PuerkitoBio/fetchbot)"

	// DefaultWorkerIdleTTL is the default time-to-live of an idle host worker goroutine.
	// If no URL is sent for a given host within this duration, this host's goroutine
	// is disposed of.
	DefaultWorkerIdleTTL = 30 * time.Second
)

// Doer defines the method required to use a type as HttpClient.
// The net/*http.Client type satisfies this interface.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// A Fetcher defines the parameters for running a web crawler.
type Fetcher struct {
	// The Handler to be called for each request. All successfully enqueued requests
	// produce a Handler call.
	Handler Handler

	// DisablePoliteness disables fetching and using the robots.txt policies of
	// hosts.
	DisablePoliteness bool

	// Default delay to use between requests to a same host if there is no robots.txt
	// crawl delay or if DisablePoliteness is true.
	CrawlDelay time.Duration

	// The *http.Client to use for the requests. If nil, defaults to the net/http
	// package's default client. Should be HTTPClient to comply with go lint, but
	// this is a breaking change, won't fix.
	HttpClient Doer

	// The user-agent string to use for robots.txt validation and URL fetching.
	UserAgent string

	// The time a host-dedicated worker goroutine can stay idle, with no Command to enqueue,
	// before it is stopped and cleared from memory.
	WorkerIdleTTL time.Duration

	// AutoClose makes the fetcher close its queue automatically once the number
	// of hosts reach 0. A host is removed once it has been idle for WorkerIdleTTL
	// duration.
	AutoClose bool

	// MaxPending limits the total number of unfinished commands across all hosts.
	// A command is unfinished from the moment it is accepted by Queue.Send until
	// its Handler returns (for any outcome, including request errors and robots.txt
	// rejections), or until it is discarded by Cancel. Queued, in-flight and
	// in-handler commands are all counted. Internal robots.txt requests do not
	// count.
	//
	// When the limit is reached, Queue.Send returns ErrQueueFull immediately and
	// the command is neither requested nor passed to the Handler. A value of zero
	// (or less) means no limit, preserving the original behavior.
	MaxPending int

	// q holds the Queue to send data to the fetcher and optionally close (stop) it.
	q *Queue
	// dbg is a channel used to push debug information.
	dbgmu     sync.Mutex
	dbg       chan *DebugInfo
	debugging bool

	// hosts maps the host names to its dedicated requests channel, and mu protects
	// concurrent access to the hosts field.
	mu    sync.Mutex
	hosts map[string]chan queueItem
}

// The DebugInfo holds information to introspect the Fetcher's state.
type DebugInfo struct {
	NumHosts int
}

// New returns an initialized Fetcher.
func New(h Handler) *Fetcher {
	return &Fetcher{
		Handler:       h,
		CrawlDelay:    DefaultCrawlDelay,
		HttpClient:    http.DefaultClient,
		UserAgent:     DefaultUserAgent,
		WorkerIdleTTL: DefaultWorkerIdleTTL,
		dbg:           make(chan *DebugInfo, 1),
	}
}

// queueItem is the internal type flowing through the queue and the per-host
// pipelines. counted reports whether the command was admitted by Queue.Send and
// thus holds one MaxPending slot until it completes or is discarded.
type queueItem struct {
	cmd     Command
	counted bool
}

// Queue offers methods to send Commands to the Fetcher, and to Stop the crawling process.
// It is safe to use from concurrent goroutines.
type Queue struct {
	ch chan queueItem

	// signal channels
	closed, cancelled, done chan struct{}

	// sendMu serializes Send's admission + enqueue with Close's close marker.
	// It must never be held while finish (or anything that acquires mu) runs.
	// Holding it across the q.ch send is safe because processQueue does not
	// need sendMu to consume q.ch.
	sendMu sync.Mutex

	// mu protects pending only. It is never held while blocking on a channel.
	mu         sync.Mutex
	maxPending int
	pending    int

	closeOnce  sync.Once
	cancelOnce sync.Once

	wg sync.WaitGroup
}

// Close closes the Queue so that no more Commands can be sent. It blocks until
// the Fetcher drains all pending commands. After the call, the Fetcher is stopped.
// Attempts to enqueue new URLs after Close has been called will always result in
// a ErrQueueClosed error.
func (q *Queue) Close() error {
	q.closeOnce.Do(func() {
		// Holding sendMu while enqueueing the close marker guarantees that every
		// command admitted before this point is already ahead of the marker in
		// q.ch, and will therefore be drained by processQueue. No other lock is
		// held here, so a sender blocked on q.ch can always be unblocked by
		// processQueue.
		q.sendMu.Lock()
		close(q.closed)
		q.ch <- queueItem{}
		q.sendMu.Unlock()

		// Wait for the queue processor and every host worker goroutine.
		q.wg.Wait()
		// Unblock any callers waiting on q.Block / Done.
		close(q.done)
	})
	<-q.done
	return nil
}

// Block blocks the current goroutine until the Queue is closed and all pending
// commands are drained.
func (q *Queue) Block() {
	<-q.done
}

// Done returns a channel that is closed when the Queue is closed (either
// via Close or Cancel). Multiple calls always return the same channel.
func (q *Queue) Done() <-chan struct{} {
	return q.done
}

// Cancel closes the Queue and drains the pending commands without processing
// them, allowing for a fast "stop immediately"-ish operation.
func (q *Queue) Cancel() error {
	q.cancelOnce.Do(func() {
		// mark the queue as cancelled
		close(q.cancelled)
	})
	// Close the Queue, that will wait for pending commands to drain
	// will unblock any callers waiting on q.Block
	return q.Close()
}

// Send enqueues a Command into the Fetcher. If the Queue has been closed, it
// returns ErrQueueClosed. If Fetcher.MaxPending is reached, it returns
// ErrQueueFull and the command is not enqueued. The Command's URL must have a Host.
// A nil return value means the command has been admitted and will either be
// processed (and its Handler called) or discarded by Cancel.
func (q *Queue) Send(c Command) error {
	if c == nil {
		return ErrEmptyHost
	}
	if u := c.URL(); u == nil || u.Host == "" {
		return ErrEmptyHost
	}

	q.sendMu.Lock()
	defer q.sendMu.Unlock()
	select {
	case <-q.closed:
		return ErrQueueClosed
	default:
	}
	q.mu.Lock()
	if q.maxPending > 0 && q.pending >= q.maxPending {
		q.mu.Unlock()
		return ErrQueueFull
	}
	// Incrementing pending and enqueueing while holding sendMu is what makes
	// "Send returned nil" equivalent to "the command was admitted": Close
	// cannot insert its close marker before an admitted command. The pending
	// counter's mutex is released before the possibly-blocking channel send.
	q.pending++
	q.mu.Unlock()
	q.ch <- queueItem{cmd: c, counted: true}
	return nil
}

// finish releases one pending slot for an admitted command that reached a
// terminal state (Handler returned) or was discarded by Cancel. It must be
// called exactly once per counted queueItem.
func (q *Queue) finish() {
	q.mu.Lock()
	if q.pending > 0 {
		q.pending--
	}
	q.mu.Unlock()
}

// SendString enqueues a method and some URL strings into the Fetcher. It returns an error
// if the URL string cannot be parsed, or if the Queue has been closed.
// The first return value is the number of URLs successfully enqueued.
func (q *Queue) SendString(method string, rawurl ...string) (int, error) {
	return q.sendWithMethod(method, rawurl)
}

// SendStringHead enqueues the URL strings to be fetched with a HEAD method.
// It returns an error if the URL string cannot be parsed, or if the Queue has been closed.
// The first return value is the number of URLs successfully enqueued.
func (q *Queue) SendStringHead(rawurl ...string) (int, error) {
	return q.sendWithMethod("HEAD", rawurl)
}

// SendStringGet enqueues the URL strings to be fetched with a GET method.
// It returns an error if the URL string cannot be parsed, or if the Queue has been closed.
// The first return value is the number of URLs successfully enqueued.
func (q *Queue) SendStringGet(rawurl ...string) (int, error) {
	return q.sendWithMethod("GET", rawurl)
}

// Parses the URL strings and enqueues them as *Cmd. It returns the number of URLs
// successfully enqueued, and an error if the URL string cannot be parsed or
// the Queue has been closed.
func (q *Queue) sendWithMethod(method string, rawurl []string) (int, error) {
	for i, v := range rawurl {
		parsed, err := url.Parse(v)
		if err != nil {
			return i, err
		}
		if err := q.Send(&Cmd{U: parsed, M: method}); err != nil {
			return i, err
		}
	}
	return len(rawurl), nil
}

// Start starts the Fetcher, and returns the Queue to use to send Commands to be fetched.
func (f *Fetcher) Start() *Queue {
	f.hosts = make(map[string]chan queueItem)

	f.q = &Queue{
		ch:         make(chan queueItem, 1),
		closed:     make(chan struct{}),
		cancelled:  make(chan struct{}),
		done:       make(chan struct{}),
		maxPending: f.MaxPending,
	}

	// Start the one and only queue processing goroutine.
	f.q.wg.Add(1)
	go f.processQueue()

	return f.q
}

// Debug returns the channel to use to receive the debugging information. It is not intended
// to be used by package users.
func (f *Fetcher) Debug() <-chan *DebugInfo {
	f.dbgmu.Lock()
	defer f.dbgmu.Unlock()
	f.debugging = true
	return f.dbg
}

// processQueue runs the queue in its own goroutine.
func (f *Fetcher) processQueue() {
loop:
	for v := range f.q.ch {
		if v.cmd == nil {
			// Close marker; Close only enqueues it after setting q.closed and
			// after all previously admitted commands. Exit the loop.
			break loop
		}
		select {
		case <-f.q.cancelled:
			// queue got cancelled, discard the command and release its slot.
			if v.counted {
				f.q.finish()
			}
			continue
		default:
			// go on
		}

		// Get the URL to enqueue
		u := v.cmd.URL()

		// Check if a channel is already started for this host
		f.mu.Lock()
		in, ok := f.hosts[u.Host]
		if !ok {
			// Start a new channel and goroutine for this host.

			var rob *url.URL
			if !f.DisablePoliteness {
				// Must send the robots.txt request.
				rob = u.ResolveReference(robotsTxtParsedPath)
			}

			// Create the infinite queue: the in channel to send on, and the out channel
			// to read from in the host's goroutine, and add to the hosts map
			var out chan queueItem
			in, out = make(chan queueItem, 1), make(chan queueItem, 1)
			f.hosts[u.Host] = in
			f.mu.Unlock()
			f.q.wg.Add(1)
			// Start the infinite queue goroutine for this host
			go sliceIQ(in, out)
			// Start the working goroutine for this host
			go f.processChan(out, u.Host)

			if !f.DisablePoliteness {
				// Enqueue the robots.txt request first. It is not a business
				// command, so it does not hold a MaxPending slot.
				in <- queueItem{cmd: robotCommand{&Cmd{U: rob, M: "GET"}}}
			}
		} else {
			f.mu.Unlock()
		}
		// Send the request
		in <- v

		// Send debug info, but do not block if full
		f.dbgmu.Lock()
		if f.debugging {
			f.mu.Lock()
			select {
			case f.dbg <- &DebugInfo{len(f.hosts)}:
			default:
			}
			f.mu.Unlock()
		}
		f.dbgmu.Unlock()
	}

	// Close all host channels now that it is impossible to send on those. Those are the `in`
	// channels of the infinite queue. It will then drain any pending events, triggering the
	// handlers for each in the worker goro, and then the infinite queue goro will terminate
	// and close the `out` channel, which in turn will terminate the worker goro.
	f.mu.Lock()
	for _, ch := range f.hosts {
		close(ch)
	}
	f.hosts = make(map[string]chan queueItem)
	f.mu.Unlock()

	f.q.wg.Done()
}

// Goroutine for a host's worker, processing requests for all its URLs.
func (f *Fetcher) processChan(ch <-chan queueItem, hostKey string) {
	var (
		agent *robotstxt.Group
		wait  <-chan time.Time
		ttl   <-chan time.Time
		delay = f.CrawlDelay
	)

loop:
	for {
		select {
		case <-f.q.cancelled:
			break loop
		case v, ok := <-ch:
			if !ok {
				// Terminate this goroutine, channel is closed
				break loop
			}

			// Wait for the prescribed delay
			if wait != nil {
				<-wait
			}

			// was it cancelled during the wait? check again
			select {
			case <-f.q.cancelled:
				// We already removed v from ch; discard it and release its slot.
				if v.counted {
					f.q.finish()
				}
				break loop
			default:
				// go on
			}

			switch r, ok := v.cmd.(robotCommand); {
			case ok:
				// This is the robots.txt request
				agent = f.getRobotAgent(r)
				// Initialize the crawl delay
				if agent != nil && agent.CrawlDelay > 0 {
					delay = agent.CrawlDelay
				}
				wait = time.After(delay)

			case agent == nil || agent.Test(v.cmd.URL().Path):
				// Path allowed, process the request
				res, err := f.doRequest(v.cmd)
				f.visit(v.cmd, res, err)
				// The command is done (success or request error); release its
				// slot only now that the Handler has returned.
				if v.counted {
					f.q.finish()
				}
				// No delay on error - the remote host was not reached
				if err == nil {
					wait = time.After(delay)
				} else {
					wait = nil
				}

			default:
				// Path disallowed by robots.txt
				f.visit(v.cmd, nil, ErrDisallowed)
				// Release only after the Handler returned.
				if v.counted {
					f.q.finish()
				}
				wait = nil
			}
			// Every time a command is received, reset the ttl channel
			ttl = time.After(f.WorkerIdleTTL)

		case <-ttl:
			// Worker has been idle for WorkerIdleTTL, terminate it
			f.mu.Lock()
			inch, ok := f.hosts[hostKey]
			delete(f.hosts, hostKey)

			// Close the queue if AutoClose is set and there are no more hosts.
			if f.AutoClose && len(f.hosts) == 0 {
				go f.q.Close()
			}
			f.mu.Unlock()
			if ok {
				close(inch)
			}
			break loop
		}
	}

	// need to drain ch until it is closed, to prevent the producer goroutine
	// from leaking. Any commands discarded here (due to Close/Cancel) release
	// their MaxPending slots.
	for v := range ch {
		if v.counted {
			f.q.finish()
		}
	}

	f.q.wg.Done()
}

// Get the robots.txt User-Agent-specific group.
func (f *Fetcher) getRobotAgent(r robotCommand) *robotstxt.Group {
	res, err := f.doRequest(r)
	if err != nil {
		// TODO: Ignore robots.txt request error?
		fmt.Fprintf(os.Stderr, "fetchbot: error fetching robots.txt: %s\n", err)
		return nil
	}
	if res.Body != nil {
		defer res.Body.Close()
	}
	robData, err := robotstxt.FromResponse(res)
	if err != nil {
		// TODO : Ignore robots.txt parse error?
		fmt.Fprintf(os.Stderr, "fetchbot: error parsing robots.txt: %s\n", err)
		return nil
	}
	return robData.FindGroup(f.UserAgent)
}

// Call the Handler for this Command. Closes the response's body.
func (f *Fetcher) visit(cmd Command, res *http.Response, err error) {
	if res != nil && res.Body != nil {
		defer res.Body.Close()
	}
	// if the Command implements Handler, call that handler, otherwise
	// dispatch to the Fetcher's Handler.
	if h, ok := cmd.(Handler); ok {
		h.Handle(&Context{Cmd: cmd, Q: f.q}, res, err)
		return
	}
	f.Handler.Handle(&Context{Cmd: cmd, Q: f.q}, res, err)
}

// Prepare and execute the request for this Command.
func (f *Fetcher) doRequest(cmd Command) (*http.Response, error) {
	req, err := http.NewRequest(cmd.Method(), cmd.URL().String(), nil)
	if err != nil {
		return nil, err
	}
	// If the Command implements some other recognized interfaces, set
	// the request accordingly (see cmd.go for the list of interfaces).
	// First, the Header values.
	if hd, ok := cmd.(HeaderProvider); ok {
		for k, v := range hd.Header() {
			req.Header[k] = v
		}
	}
	// BasicAuth has higher priority than an Authorization header set by
	// a HeaderProvider.
	if ba, ok := cmd.(BasicAuthProvider); ok {
		req.SetBasicAuth(ba.BasicAuth())
	}
	// Cookies are added to the request, even if some cookies were set
	// by a HeaderProvider.
	if ck, ok := cmd.(CookiesProvider); ok {
		for _, c := range ck.Cookies() {
			req.AddCookie(c)
		}
	}
	// For the body of the request, ReaderProvider has higher priority
	// than ValuesProvider.
	if rd, ok := cmd.(ReaderProvider); ok {
		rdr := rd.Reader()
		rc, ok := rdr.(io.ReadCloser)
		if !ok {
			rc = ioutil.NopCloser(rdr)
		}
		req.Body = rc
	} else if val, ok := cmd.(ValuesProvider); ok {
		v := val.Values()
		req.Body = ioutil.NopCloser(strings.NewReader(v.Encode()))
		if req.Header.Get("Content-Type") == "" {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
	}
	// If there was no User-Agent implicitly set by the HeaderProvider,
	// set it to the default value.
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", f.UserAgent)
	}
	// Do the request.
	res, err := f.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}
	return res, nil
}
