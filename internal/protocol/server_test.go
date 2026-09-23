package protocol_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pb "github.com/SchwarzDigits/sqlite-remote-server/internal/gen/sqlite_remote/v1"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/protocol"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/store/memory"
)

const (
	testPageSize = 512
	testDB       = "keystore"
	testTimeout  = 5 * time.Second
)

var t0 = time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type env struct {
	url    string
	clock  *clock
	server *protocol.Server
}

func start(t *testing.T, modify ...func(*protocol.Options)) *env {
	t.Helper()
	clk := &clock{now: t0}
	opts := protocol.Options{
		Store:          memory.New(),
		Log:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxFrameBytes:  1 << 20,
		PingInterval:   10 * time.Second,
		LeaseTTL:       30 * time.Second,
		MaxCommitBytes: 1 << 24,
		HelloTimeout:   testTimeout,
		ServerID:       testServerID,
		Now:            clk.Now,
	}
	for _, m := range modify {
		m(&opts)
	}
	server := protocol.NewServer(opts)
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()
		_ = server.Shutdown(ctx)
		httpServer.Close()
	})
	return &env{url: "ws" + strings.TrimPrefix(httpServer.URL, "http"), clock: clk, server: server}
}

type client struct {
	t        *testing.T
	ws       *websocket.Conn
	lastID   uint64
	instance []byte
}

func (e *env) connect(t *testing.T, instance byte) *client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, e.url, nil)
	require.NoError(t, err)
	ws.SetReadLimit(4 << 20)
	t.Cleanup(func() { _ = ws.CloseNow() })
	return &client{t: t, ws: ws, instance: bytes.Repeat([]byte{instance}, 16)}
}

func (c *client) send(frame *pb.ClientFrame) uint64 {
	c.t.Helper()
	c.lastID++
	frame.RequestId = c.lastID
	data, err := proto.Marshal(frame)
	require.NoError(c.t, err)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	require.NoError(c.t, c.ws.Write(ctx, websocket.MessageBinary, data))
	return c.lastID
}

func (c *client) recv() *pb.ServerFrame {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	kind, data, err := c.ws.Read(ctx)
	require.NoError(c.t, err)
	require.Equal(c.t, websocket.MessageBinary, kind)
	frame := &pb.ServerFrame{}
	require.NoError(c.t, proto.Unmarshal(data, frame))
	return frame
}

// call sends frame and returns the response. The response must carry the frame's request ID.
func (c *client) call(frame *pb.ClientFrame) *pb.ServerFrame {
	c.t.Helper()
	id := c.send(frame)
	answer := c.recv()
	require.Equal(c.t, id, answer.GetRequestId(), "answer %v", answer)
	return answer
}

// hello logs in with the key for name (see keyFor). The same name gives the same key and therefore the same
// subject. The name itself is never sent to the server.
func (c *client) hello(name string) *pb.HelloOk {
	c.t.Helper()
	public, private := keyFor(name)
	answer := c.login(public, private)
	require.NotNil(c.t, answer.GetHelloOk(), "answer %v", answer)
	return answer.GetHelloOk()
}

func openFrame(db string, create, takeover bool, resume *pb.Resume) *pb.ClientFrame {
	return &pb.ClientFrame{Body: &pb.ClientFrame_Open{Open: &pb.Open{
		DbId: db, PageSize: testPageSize, CreateIfMissing: create, Takeover: takeover, Resume: resume,
	}}}
}

func (c *client) open(db string, create, takeover bool) *pb.Opened {
	c.t.Helper()
	answer := c.call(openFrame(db, create, takeover, nil))
	require.NotNil(c.t, answer.GetOpened(), "answer %v", answer)
	return answer.GetOpened()
}

// commitFrame returns a commit that consists of a single part.
func commitFrame(epoch, base, pageCount uint64, id []byte, blocks ...*pb.Block) *pb.ClientFrame {
	return commitPart(epoch, base, pageCount, id, 0, false, blocks...)
}

func commitPart(epoch, base, pageCount uint64, id []byte, part uint32, more bool, blocks ...*pb.Block) *pb.ClientFrame {
	return &pb.ClientFrame{Body: &pb.ClientFrame_Commit{Commit: &pb.Commit{
		DbId: testDB, CommitId: id, LeaseEpoch: epoch, BaseVersion: base, PageCount: pageCount, Blocks: blocks,
		More: more, Part: part,
	}}}
}

func fetchFrame(version, first, count uint64) *pb.ClientFrame {
	return &pb.ClientFrame{Body: &pb.ClientFrame_Fetch{Fetch: &pb.Fetch{
		DbId: testDB, Version: version, FirstBlock: first, Count: count,
	}}}
}

func pingFrame() *pb.ClientFrame {
	return &pb.ClientFrame{Body: &pb.ClientFrame_Ping{Ping: &pb.Ping{ClientTimeMs: 42}}}
}

func block(index uint64, fill byte) *pb.Block {
	return &pb.Block{Index: index, Data: bytes.Repeat([]byte{fill}, testPageSize)}
}

func commitIDOf(fill byte) []byte {
	return bytes.Repeat([]byte{fill}, 16)
}

func requireError(t *testing.T, frame *pb.ServerFrame, code pb.ErrorCode) *pb.Error {
	t.Helper()
	require.NotNil(t, frame.GetError(), "frame %v", frame)
	require.Equal(t, code, frame.GetError().GetCode(), "error %v", frame.GetError())
	return frame.GetError()
}

// fetchAll fetches count blocks starting at block 0 and returns the Pages frames and the blocks.
func (c *client) fetchAll(version, count uint64) ([]*pb.Pages, [][]byte) {
	c.t.Helper()
	id := c.send(fetchFrame(version, 0, count))
	var frames []*pb.Pages
	var blocks [][]byte
	for {
		answer := c.recv()
		require.Equal(c.t, id, answer.GetRequestId())
		pages := answer.GetPages()
		require.NotNil(c.t, pages, "answer %v", answer)
		require.Equal(c.t, uint64(len(blocks)), pages.GetFirstBlock())
		frames = append(frames, pages)
		blocks = append(blocks, pages.GetBlocks()...)
		if pages.GetLast() {
			return frames, blocks
		}
	}
}

func TestFirstFrameMustBeHello(t *testing.T) {
	e := start(t)
	c := e.connect(t, 0xa)
	answer := c.call(openFrame(testDB, true, false, nil))
	requireError(t, answer, pb.ErrorCode_ERROR_CODE_UNAUTHENTICATED)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	_, _, err := c.ws.Read(ctx)
	require.Equal(t, websocket.StatusPolicyViolation, websocket.CloseStatus(err))
}

func TestHelloOkReportsLimits(t *testing.T) {
	e := start(t)
	ok := e.connect(t, 0xa).hello("alice")
	require.Equal(t, &pb.HelloOk{
		ProtocolVersion: protocol.Version, MaxFrameBytes: 1 << 20, PingIntervalMs: 10_000, LeaseTtlMs: 30_000,
	}, ok)
}

func TestOpenCommitFetch(t *testing.T) {
	e := start(t)
	c := e.connect(t, 0xa)
	c.hello("alice")
	opened := c.open(testDB, true, false)
	require.Equal(t, uint64(1), opened.GetLeaseEpoch())
	require.Equal(t, uint64(0), opened.GetVersion())
	require.Equal(t, uint32(testPageSize), opened.GetPageSize())

	answer := c.call(commitFrame(1, 0, 3, commitIDOf(1), block(0, 0xa0), block(2, 0xa2)))
	require.Equal(t, &pb.CommitAck{CommitId: commitIDOf(1), Version: 1}, answer.GetCommitAck(), "answer %v", answer)

	_, blocks := c.fetchAll(1, 3)
	require.Equal(t, [][]byte{block(0, 0xa0).Data, make([]byte, testPageSize), block(2, 0xa2).Data}, blocks)
}

func TestMultiPartCommitGetsOneResponse(t *testing.T) {
	e := start(t)
	c := e.connect(t, 0xa)
	c.hello("alice")
	c.open(testDB, true, false)

	c.send(commitPart(1, 0, 2, commitIDOf(1), 0, true, block(0, 0xa0)))
	last := c.send(commitPart(1, 0, 2, commitIDOf(1), 1, false, block(1, 0xa1)))
	answer := c.recv()
	require.Equal(t, last, answer.GetRequestId(), "only the last part gets a response: %v", answer)
	require.Equal(t, uint64(1), answer.GetCommitAck().GetVersion())

	_, blocks := c.fetchAll(1, 2)
	require.Equal(t, [][]byte{block(0, 0xa0).Data, block(1, 0xa1).Data}, blocks)
}

func TestRepeatedCommitIsAcknowledgedAgain(t *testing.T) {
	e := start(t)
	c := e.connect(t, 0xa)
	c.hello("alice")
	c.open(testDB, true, false)
	for range 2 {
		answer := c.call(commitFrame(1, 0, 1, commitIDOf(1), block(0, 0xa0)))
		require.Equal(t, uint64(1), answer.GetCommitAck().GetVersion(), "answer %v", answer)
	}
}

func TestStaleBaseVersionConflicts(t *testing.T) {
	e := start(t)
	c := e.connect(t, 0xa)
	c.hello("alice")
	c.open(testDB, true, false)
	c.call(commitFrame(1, 0, 1, commitIDOf(1), block(0, 0xa0)))

	answer := c.call(commitFrame(1, 0, 1, commitIDOf(2), block(0, 0xb0)))
	require.Equal(t, uint64(1), requireError(t, answer, pb.ErrorCode_ERROR_CODE_VERSION_CONFLICT).GetCurrentVersion())
}

func TestHeldLeaseIsReported(t *testing.T) {
	e := start(t)
	a := e.connect(t, 0xa)
	a.hello("alice")
	a.open(testDB, true, false)

	b := e.connect(t, 0xb)
	b.hello("alice")
	answer := b.call(openFrame(testDB, false, false, nil))
	since := requireError(t, answer, pb.ErrorCode_ERROR_CODE_LEASE_HELD).GetLeaseHolderSinceMs()
	require.Equal(t, uint64(t0.UnixMilli()), since)
}

func TestTakeoverRevokesOldHolder(t *testing.T) {
	e := start(t)
	a := e.connect(t, 0xa)
	a.hello("alice")
	a.open(testDB, true, false)

	b := e.connect(t, 0xb)
	b.hello("alice")
	opened := b.open(testDB, false, true)
	require.Equal(t, uint64(2), opened.GetLeaseEpoch())

	push := a.recv()
	require.Equal(t, uint64(0), push.GetRequestId())
	require.Equal(t, &pb.LeaseRevoked{DbId: testDB, NewLeaseEpoch: 2}, push.GetLeaseRevoked(), "push %v", push)

	answer := a.call(commitFrame(1, 0, 1, commitIDOf(1), block(0, 0xa0)))
	requireError(t, answer, pb.ErrorCode_ERROR_CODE_FENCED)
}

func TestResumeOnNewConnection(t *testing.T) {
	e := start(t)
	a := e.connect(t, 0xa)
	a.hello("alice")
	opened := a.open(testDB, true, false)
	a.call(commitFrame(1, 0, 1, commitIDOf(1), block(0, 0xa0)))
	require.NoError(t, a.ws.Close(websocket.StatusNormalClosure, ""))

	again := e.connect(t, 0xa)
	again.hello("alice")
	answer := again.call(openFrame(testDB, false, false, &pb.Resume{
		LeaseId: opened.GetLeaseId(), LeaseEpoch: opened.GetLeaseEpoch(), KnownVersion: 1,
	}))
	resumed := answer.GetOpened()
	require.NotNil(t, resumed, "answer %v", answer)
	require.Equal(t, opened.GetLeaseId(), resumed.GetLeaseId())
	require.Equal(t, opened.GetLeaseEpoch(), resumed.GetLeaseEpoch())
	require.Equal(t, uint64(1), resumed.GetVersion())
	require.Equal(t, commitIDOf(1), resumed.GetLastCommitId())

	ack := again.call(commitFrame(1, 1, 1, commitIDOf(2), block(0, 0xb0)))
	require.Equal(t, uint64(2), ack.GetCommitAck().GetVersion(), "answer %v", ack)
}

func TestFetchIsSplitAcrossFrames(t *testing.T) {
	e := start(t, func(o *protocol.Options) { o.MaxFrameBytes = 4096 })
	c := e.connect(t, 0xa)
	c.hello("alice")
	c.open(testDB, true, false)
	// The commit must fit the frame limit too, so it is sent in parts of five blocks.
	for first := uint64(0); first < 20; first += 5 {
		var blocks []*pb.Block
		for i := first; i < first+5; i++ {
			blocks = append(blocks, block(i, byte(i)))
		}
		more := first+5 < 20
		id := c.send(commitPart(1, 0, 20, commitIDOf(1), uint32(first/5), more, blocks...))
		if !more {
			answer := c.recv()
			require.Equal(t, id, answer.GetRequestId())
			require.Equal(t, uint64(1), answer.GetCommitAck().GetVersion(), "answer %v", answer)
		}
	}

	frames, fetched := c.fetchAll(1, 20)
	require.Greater(t, len(frames), 1)
	for i, frame := range frames {
		require.Equal(t, i == len(frames)-1, frame.GetLast())
		data, err := proto.Marshal(&pb.ServerFrame{RequestId: 1, Body: &pb.ServerFrame_Pages{Pages: frame}})
		require.NoError(t, err)
		require.LessOrEqual(t, len(data), 4096)
	}
	require.Len(t, fetched, 20)
	for i, data := range fetched {
		require.Equal(t, block(uint64(i), byte(i)).Data, data)
	}
}

func TestEmptyFetchReturnsOneFrame(t *testing.T) {
	e := start(t)
	c := e.connect(t, 0xa)
	c.hello("alice")
	c.open(testDB, true, false)
	frames, blocks := c.fetchAll(0, 0)
	require.Len(t, frames, 1)
	require.Empty(t, blocks)
}

func TestPingRenewsLease(t *testing.T) {
	e := start(t)
	a := e.connect(t, 0xa)
	a.hello("alice")
	a.open(testDB, true, false)
	e.clock.Advance(25 * time.Second)
	require.Equal(t, uint64(42), a.call(pingFrame()).GetPong().GetClientTimeMs())

	e.clock.Advance(25 * time.Second)
	b := e.connect(t, 0xb)
	b.hello("alice")
	requireError(t, b.call(openFrame(testDB, false, false, nil)), pb.ErrorCode_ERROR_CODE_LEASE_HELD)

	e.clock.Advance(10 * time.Second)
	require.Equal(t, uint64(2), b.open(testDB, false, false).GetLeaseEpoch(), "the renewed lease must have expired by now")
}

func TestCloseDbReleasesLease(t *testing.T) {
	e := start(t)
	a := e.connect(t, 0xa)
	a.hello("alice")
	opened := a.open(testDB, true, false)
	answer := a.call(&pb.ClientFrame{Body: &pb.ClientFrame_CloseDb{CloseDb: &pb.CloseDb{
		DbId: testDB, LeaseEpoch: opened.GetLeaseEpoch(),
	}}})
	require.NotNil(t, answer.GetOk(), "answer %v", answer)

	b := e.connect(t, 0xb)
	b.hello("alice")
	require.Equal(t, uint64(2), b.open(testDB, false, false).GetLeaseEpoch())
	requireError(t, a.call(fetchFrame(0, 0, 0)), pb.ErrorCode_ERROR_CODE_BAD_REQUEST)
}

func TestCommitOverSizeLimitIsRejected(t *testing.T) {
	e := start(t, func(o *protocol.Options) { o.MaxCommitBytes = 2 * testPageSize })
	c := e.connect(t, 0xa)
	c.hello("alice")
	c.open(testDB, true, false)
	answer := c.call(commitFrame(1, 0, 3, commitIDOf(1), block(0, 1), block(1, 2), block(2, 3)))
	requireError(t, answer, pb.ErrorCode_ERROR_CODE_TOO_LARGE)
}

func TestMismatchedCommitPartIsRejected(t *testing.T) {
	e := start(t)
	c := e.connect(t, 0xa)
	c.hello("alice")
	c.open(testDB, true, false)
	c.send(commitPart(1, 0, 2, commitIDOf(1), 0, true, block(0, 0xa0)))
	answer := c.call(commitPart(1, 5, 2, commitIDOf(1), 1, false, block(1, 0xa1)))
	requireError(t, answer, pb.ErrorCode_ERROR_CODE_BAD_REQUEST)

	// No part of the rejected commit was applied.
	_, blocks := c.fetchAll(0, 0)
	require.Empty(t, blocks)
}

// The client sends all parts of a commit without waiting for responses. If a middle part is rejected, the following
// parts must be rejected too. Otherwise the server would apply a partial commit.
func TestPartsAfterRejectedPartAreNotApplied(t *testing.T) {
	e := start(t)
	c := e.connect(t, 0xa)
	c.hello("alice")
	c.open(testDB, true, false)

	c.send(commitPart(1, 0, 3, commitIDOf(1), 0, true, block(0, 0xa0)))
	// Part 1 has a different base version, so it does not continue part 0 and is rejected.
	refused := c.send(commitPart(1, 5, 3, commitIDOf(1), 1, true, block(1, 0xa1)))
	answer := c.recv()
	require.Equal(t, refused, answer.GetRequestId())
	requireError(t, answer, pb.ErrorCode_ERROR_CODE_BAD_REQUEST)

	// Part 2 is valid by itself, but no commit is pending anymore.
	last := c.send(commitPart(1, 0, 3, commitIDOf(1), 2, false, block(2, 0xa2)))
	answer = c.recv()
	require.Equal(t, last, answer.GetRequestId())
	requireError(t, answer, pb.ErrorCode_ERROR_CODE_BAD_REQUEST)

	_, blocks := c.fetchAll(0, 0)
	require.Empty(t, blocks, "version 0 must still be current")
}

func TestShutdownClosesWithGoingAway(t *testing.T) {
	e := start(t)
	c := e.connect(t, 0xa)
	c.hello("alice")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	go func() { _ = e.server.Shutdown(ctx) }()
	_, _, err := c.ws.Read(ctx)
	require.Equal(t, websocket.StatusGoingAway, websocket.CloseStatus(err))
}

// Browsers send an Origin header, other clients do not. Without AllowedOrigins, only pages from the server's own
// host may connect.
func TestBrowserOriginCheck(t *testing.T) {
	dialFrom := func(t *testing.T, e *env, origin string) error {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()
		ws, _, err := websocket.Dial(ctx, e.url, &websocket.DialOptions{
			HTTPHeader: http.Header{"Origin": []string{origin}},
		})
		if err == nil {
			_ = ws.CloseNow()
		}
		return err
	}

	t.Run("other host rejected by default", func(t *testing.T) {
		e := start(t)
		require.Error(t, dialFrom(t, e, "https://client.example"))
	})

	t.Run("allowed host accepted", func(t *testing.T) {
		e := start(t, func(o *protocol.Options) { o.AllowedOrigins = []string{"client.example"} })
		require.NoError(t, dialFrom(t, e, "https://client.example"))
		require.Error(t, dialFrom(t, e, "https://somewhere.else"), "other hosts must still be rejected")
	})

	t.Run("client without origin accepted", func(t *testing.T) {
		e := start(t)
		e.connect(t, 1)
	})
}

func TestChangedListsChangedBlocks(t *testing.T) {
	e := start(t)
	c := e.connect(t, 0xa)
	c.hello("alice")
	c.open(testDB, true, false)
	c.call(commitFrame(1, 0, 8, commitIDOf(1), block(0, 0xa0), block(1, 0xa1)))
	c.call(commitFrame(1, 1, 8, commitIDOf(2), block(4, 0xb4)))

	answer := c.call(&pb.ClientFrame{Body: &pb.ClientFrame_Changed{Changed: &pb.Changed{
		DbId: testDB, FromVersion: 1,
	}}})
	changes := answer.GetChanges()
	require.NotNil(t, changes, "answer %v", answer)
	require.True(t, changes.GetComplete())
	require.Equal(t, uint64(1), changes.GetFromVersion())
	require.Equal(t, uint64(2), changes.GetToVersion())
	require.Equal(t, []uint64{4}, changes.GetBlocks(), "only the blocks changed by the second commit")
}

func TestChangedOnUnopenedDatabaseFails(t *testing.T) {
	e := start(t)
	c := e.connect(t, 0xa)
	c.hello("alice")

	answer := c.call(&pb.ClientFrame{Body: &pb.ClientFrame_Changed{Changed: &pb.Changed{
		DbId: "never-opened", FromVersion: 0,
	}}})
	require.NotNil(t, answer.GetError(), "answer %v", answer)
}

func TestFetchRangesAreAnsweredInOrder(t *testing.T) {
	e := start(t)
	c := e.connect(t, 0xa)
	c.hello("alice")
	c.open(testDB, true, false)
	c.call(commitFrame(1, 0, 6, commitIDOf(1),
		block(0, 0xa0), block(1, 0xa1), block(3, 0xa3), block(5, 0xa5)))

	// Two ranges in one request: blocks 0 to 1 and block 5.
	id := c.send(&pb.ClientFrame{Body: &pb.ClientFrame_Fetch{Fetch: &pb.Fetch{
		DbId: testDB, Version: 1,
		Ranges: []*pb.Range{{FirstBlock: 0, Count: 2}, {FirstBlock: 5, Count: 1}},
	}}})

	var got []*pb.Pages
	for {
		answer := c.recv()
		require.Equal(t, id, answer.GetRequestId())
		pages := answer.GetPages()
		require.NotNil(t, pages, "answer %v", answer)
		got = append(got, pages)
		if pages.GetLast() {
			break
		}
	}
	require.Len(t, got, 2, "one frame per range")
	require.Equal(t, uint64(0), got[0].GetFirstBlock())
	require.Equal(t, [][]byte{block(0, 0xa0).Data, block(1, 0xa1).Data}, got[0].GetBlocks())
	require.Equal(t, uint64(5), got[1].GetFirstBlock(), "the second frame must start at block 5")
	require.Equal(t, [][]byte{block(5, 0xa5).Data}, got[1].GetBlocks())
}

func TestFetchRejectsRangesOutOfOrder(t *testing.T) {
	e := start(t)
	c := e.connect(t, 0xa)
	c.hello("alice")
	c.open(testDB, true, false)
	c.call(commitFrame(1, 0, 6, commitIDOf(1), block(0, 0xa0)))

	answer := c.call(&pb.ClientFrame{Body: &pb.ClientFrame_Fetch{Fetch: &pb.Fetch{
		DbId: testDB, Version: 1,
		Ranges: []*pb.Range{{FirstBlock: 4, Count: 2}, {FirstBlock: 0, Count: 2}},
	}}})
	require.NotNil(t, answer.GetError(), "answer %v", answer)
	require.Equal(t, pb.ErrorCode_ERROR_CODE_BAD_REQUEST, answer.GetError().GetCode())
}
