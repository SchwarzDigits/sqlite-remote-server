package protocol

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	pb "github.com/SchwarzDigits/sqlite-remote-server/internal/gen/sqlite_remote/v1"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/store"
)

const (
	commitIDBytes      = 16
	maxInstanceIDBytes = 64
	maxDBIDBytes       = 128
	writeTimeout       = 10 * time.Second
	// pagesFrameOverhead is the space in a Pages frame outside the block data. pagesBlockOverhead is the tag and
	// length prefix of each block.
	pagesFrameOverhead = 64
	pagesBlockOverhead = 8
	// changesFrameOverhead is the space in a Changes frame outside the list of block indexes.
	changesFrameOverhead = 64
	// maxFetchRanges limits the number of ranges in one Fetch request. It is large enough to update a local copy
	// with one request and bounds the number of store lookups per request.
	maxFetchRanges = 4096
)

var (
	errNotBinary = errors.New("text frame")
	errMalformed = errors.New("malformed frame")
)

// connection serves one WebSocket connection. Frames are handled one at a time on the reading goroutine, so the
// connection state needs no lock. LeaseRevoked pushes are sent from other goroutines and only write to the socket.
type connection struct {
	s   *Server
	ws  *websocket.Conn
	log *slog.Logger
	// ctx is canceled when the connection ends. Pushes use it.
	ctx context.Context

	subject  string
	instance []byte
	dbs      map[string]*openDB
}

type openDB struct {
	key      store.Key
	epoch    uint64
	pageSize uint32
	pending  *pendingCommit
	// renewed is the time the lease was last extended in the store: at open, by a commit or by a ping. See
	// renewDue.
	renewed time.Time
}

// renewDue reports whether a ping should extend the lease in the store.
//
// A ping extends the lease only if at least half of LeaseTTL has passed since the last extension. Extending on every
// ping costs one store write per ping: 2,000 idle clients caused about 400 writes per second. An idle client pings
// about every half ping interval, so the extension happens at most that long after the halfway point, well before
// the lease expires. A commit also extends the lease.
func (c *connection) renewDue(db *openDB, now time.Time) bool {
	return now.Sub(db.renewed) >= c.s.opts.LeaseTTL/2
}

// pendingCommit collects the parts of a commit until the last part arrives.
type pendingCommit struct {
	header *pb.Commit
	// nextPart is the part number the next frame must carry.
	nextPart uint32
	blocks   []store.Block
	size     uint64
}

func newConnection(s *Server, ws *websocket.Conn) *connection {
	return &connection{s: s, ws: ws, log: s.opts.Log, dbs: make(map[string]*openDB)}
}

func (c *connection) serve(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	c.ctx = ctx
	go func() {
		select {
		case <-c.s.stop:
			_ = c.ws.Close(websocket.StatusGoingAway, "server shutting down")
		case <-ctx.Done():
		}
	}()
	defer func() {
		// The leases stay in the store, so the client can resume them on a new connection.
		for _, db := range c.dbs {
			c.s.holders.remove(db.key, c)
		}
	}()

	status, reason := c.run(ctx)
	_ = c.ws.Close(status, reason)
}

func (c *connection) run(ctx context.Context) (websocket.StatusCode, string) {
	frame, err := c.read(ctx, c.s.opts.HelloTimeout)
	if err != nil {
		return c.readFailed(err)
	}
	if err := c.hello(ctx, frame); err != nil {
		return websocket.StatusPolicyViolation, err.Error()
	}
	for {
		frame, err := c.read(ctx, 3*c.s.opts.PingInterval)
		if err != nil {
			return c.readFailed(err)
		}
		c.handle(ctx, frame)
	}
}

func (c *connection) read(ctx context.Context, timeout time.Duration) (*pb.ClientFrame, error) {
	readCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	kind, data, err := c.ws.Read(readCtx)
	if err != nil {
		return nil, err
	}
	if kind != websocket.MessageBinary {
		return nil, errNotBinary
	}
	frame := &pb.ClientFrame{}
	if err := proto.Unmarshal(data, frame); err != nil {
		return nil, fmt.Errorf("%w: %w", errMalformed, err)
	}
	return frame, nil
}

func (c *connection) readFailed(err error) (websocket.StatusCode, string) {
	switch {
	case errors.Is(err, errNotBinary):
		return websocket.StatusUnsupportedData, "binary frames only"
	case errors.Is(err, errMalformed):
		return websocket.StatusInvalidFramePayloadData, "malformed frame"
	case errors.Is(err, context.DeadlineExceeded):
		return websocket.StatusPolicyViolation, "no frame in time"
	default:
		// The client closed the connection, or the connection failed.
		return websocket.StatusNormalClosure, ""
	}
}

func (c *connection) hello(ctx context.Context, frame *pb.ClientFrame) error {
	hello := frame.GetHello()
	var err error
	switch {
	case hello == nil:
		err = unauthenticated("the first frame must be a Hello")
	case hello.GetProtocolVersion() != Version:
		err = store.BadRequest("protocol version %d, this server speaks %d", hello.GetProtocolVersion(), Version)
	case len(hello.GetInstanceId()) == 0 || len(hello.GetInstanceId()) > maxInstanceIDBytes:
		err = store.BadRequest("instance id must have 1 to %d bytes", maxInstanceIDBytes)
	}
	if err != nil {
		c.fail(ctx, frame.GetRequestId(), err)
		return err
	}

	// HelloOk and login errors carry the request ID of the frame that completed or failed the login.
	subject, answering, err := c.identify(ctx, frame.GetRequestId(), hello)
	if err != nil {
		c.fail(ctx, answering, err)
		return err
	}

	c.subject = subject
	c.instance = bytes.Clone(hello.GetInstanceId())
	c.log = c.log.With("subject", c.subject)
	c.send(ctx, &pb.ServerFrame{RequestId: answering, Body: &pb.ServerFrame_HelloOk{HelloOk: &pb.HelloOk{
		ProtocolVersion: Version,
		MaxFrameBytes:   c.s.opts.MaxFrameBytes,
		PingIntervalMs:  uint32(c.s.opts.PingInterval.Milliseconds()),
		LeaseTtlMs:      uint32(c.s.opts.LeaseTTL.Milliseconds()),
	}}})
	return nil
}

// identify authenticates the client. It returns the subject and the request ID of the frame to answer.
//
// The server sends a challenge with a nonce. The client signs the transcript with the private key that belongs to
// the public key in its Hello. The subject is derived from the public key. A client cannot choose its subject.
func (c *connection) identify(ctx context.Context, id uint64, hello *pb.Hello) (string, uint64, error) {
	if c.s.opts.ServerID == "" {
		// Fail closed. Without a server ID the transcript cannot bind the signature to this server.
		return "", id, errors.New("no server ID configured, all logins are rejected")
	}
	if len(hello.GetPublicKey()) == 0 {
		return "", id, unauthenticated("no public key")
	}

	nonce, err := newNonce()
	if err != nil {
		return "", id, err
	}
	deadline := c.s.opts.Now().Add(c.challengeTTL())
	c.send(ctx, &pb.ServerFrame{RequestId: id, Body: &pb.ServerFrame_Challenge{Challenge: &pb.Challenge{
		Nonce:       nonce,
		ServerId:    c.s.opts.ServerID,
		ExpiresAtMs: uint64(deadline.UnixMilli()),
	}}})

	// The next frame must be the Proof, and it must arrive before the challenge expires.
	answer, err := c.read(ctx, c.challengeTTL())
	if err != nil {
		return "", id, unauthenticated("no proof: %v", err)
	}
	answering := answer.GetRequestId()
	proof := answer.GetProof()
	if proof == nil {
		return "", answering, unauthenticated("expected a Proof, got %T", answer.GetBody())
	}
	if c.s.opts.Now().After(deadline) {
		return "", answering, unauthenticated("the challenge expired")
	}
	signed := transcript(c.s.opts.ServerID, nonce, hello.GetInstanceId(), hello.GetSigAlg(), hello.GetPublicKey())
	if err := checkProof(hello.GetSigAlg(), hello.GetPublicKey(), signed, proof.GetSignature()); err != nil {
		return "", answering, err
	}
	return subjectOf(hello.GetSigAlg(), hello.GetPublicKey()), answering, nil
}

func (c *connection) challengeTTL() time.Duration {
	if c.s.opts.ChallengeTTL > 0 {
		return c.s.opts.ChallengeTTL
	}
	return defaultChallengeTTL
}

func (c *connection) handle(ctx context.Context, frame *pb.ClientFrame) {
	id := frame.GetRequestId()
	if id == 0 {
		c.fail(ctx, 0, store.BadRequest("request id must be greater than 0"))
		return
	}
	switch body := frame.GetBody().(type) {
	case *pb.ClientFrame_Open:
		c.open(ctx, id, body.Open)
	case *pb.ClientFrame_Fetch:
		c.fetch(ctx, id, body.Fetch)
	case *pb.ClientFrame_Commit:
		c.commit(ctx, id, body.Commit)
	case *pb.ClientFrame_Changed:
		c.changed(ctx, id, body.Changed)
	case *pb.ClientFrame_Ping:
		c.ping(ctx, id, body.Ping)
	case *pb.ClientFrame_CloseDb:
		c.closeDB(ctx, id, body.CloseDb)
	case *pb.ClientFrame_Hello:
		c.fail(ctx, id, store.BadRequest("Hello was already sent"))
	default:
		c.fail(ctx, id, store.BadRequest("empty frame"))
	}
}

func (c *connection) open(ctx context.Context, id uint64, req *pb.Open) {
	if err := checkDBID(req.GetDbId()); err != nil {
		c.fail(ctx, id, err)
		return
	}
	if _, ok := c.dbs[req.GetDbId()]; ok {
		c.fail(ctx, id, store.BadRequest("database %q is already open on this connection", req.GetDbId()))
		return
	}
	key := store.Key{Subject: c.subject, DBID: req.GetDbId()}
	var resume *store.Resume
	if r := req.GetResume(); r != nil {
		resume = &store.Resume{LeaseID: r.GetLeaseId(), LeaseEpoch: r.GetLeaseEpoch()}
	}
	now := c.s.opts.Now()
	res, err := c.s.opts.Store.Open(ctx, store.OpenRequest{
		Key:        key,
		InstanceID: c.instance,
		PageSize:   req.GetPageSize(),
		Create:     req.GetCreateIfMissing(),
		Takeover:   req.GetTakeover(),
		Resume:     resume,
		Now:        now,
		TTL:        c.s.opts.LeaseTTL,
	})
	if err != nil {
		c.fail(ctx, id, err)
		return
	}

	dbID := req.GetDbId()
	c.dbs[dbID] = &openDB{key: key, epoch: res.Lease.Epoch, pageSize: res.State.PageSize, renewed: now}
	c.s.holders.set(key, res.Lease.Epoch, c, func(newEpoch uint64) {
		c.send(c.ctx, &pb.ServerFrame{Body: &pb.ServerFrame_LeaseRevoked{LeaseRevoked: &pb.LeaseRevoked{
			DbId: dbID, NewLeaseEpoch: newEpoch,
		}}})
	})
	c.send(ctx, &pb.ServerFrame{RequestId: id, Body: &pb.ServerFrame_Opened{Opened: &pb.Opened{
		LeaseId:      res.Lease.ID,
		LeaseEpoch:   res.Lease.Epoch,
		Version:      res.State.Version,
		PageSize:     res.State.PageSize,
		PageCount:    res.State.PageCount,
		LastCommitId: res.State.LastCommitID,
	}}})
}

func (c *connection) fetch(ctx context.Context, id uint64, req *pb.Fetch) {
	db, err := c.openDB(req.GetDbId())
	if err != nil {
		c.fail(ctx, id, err)
		return
	}
	ranges := req.GetRanges()
	if len(ranges) == 0 {
		ranges = []*pb.Range{{FirstBlock: req.GetFirstBlock(), Count: req.GetCount()}}
	}
	if len(ranges) > maxFetchRanges {
		c.fail(ctx, id, store.BadRequest("fetch has %d ranges, at most %d are allowed", len(ranges), maxFetchRanges))
		return
	}
	// Ranges must be sorted and must not overlap, so the blocks of the answer are unambiguous.
	var reached uint64
	for n, r := range ranges {
		if n > 0 && r.GetFirstBlock() < reached {
			c.fail(ctx, id, store.BadRequest("range %d starts at %d, before the end of the previous range",
				n, r.GetFirstBlock()))
			return
		}
		reached = r.GetFirstBlock() + r.GetCount()
	}

	runs := make([][][]byte, 0, len(ranges))
	for _, r := range ranges {
		blocks, err := c.s.opts.Store.Fetch(ctx, db.key, req.GetVersion(), r.GetFirstBlock(), r.GetCount())
		if err != nil {
			c.fail(ctx, id, err)
			return
		}
		runs = append(runs, blocks)
	}

	perFrame := max(1, int((c.s.opts.MaxFrameBytes-pagesFrameOverhead)/(db.pageSize+pagesBlockOverhead)))
	// A frame never spans two ranges, so FirstBlock determines the index of every block in the frame.
	for n, blocks := range runs {
		first := ranges[n].GetFirstBlock()
		lastRun := n == len(runs)-1
		start := 0
		for {
			end := min(start+perFrame, len(blocks))
			c.send(ctx, &pb.ServerFrame{RequestId: id, Body: &pb.ServerFrame_Pages{Pages: &pb.Pages{
				FirstBlock: first + uint64(start),
				Blocks:     blocks[start:end],
				Last:       lastRun && end == len(blocks),
			}}})
			if end == len(blocks) {
				break
			}
			start = end
		}
	}
}

// changed returns the blocks changed since a version. Clients use it to update a stale local copy.
func (c *connection) changed(ctx context.Context, id uint64, req *pb.Changed) {
	db, err := c.openDB(req.GetDbId())
	if err != nil {
		c.fail(ctx, id, err)
		return
	}
	set, err := c.s.opts.Store.Changes(ctx, db.key, req.GetFromVersion())
	if err != nil {
		c.fail(ctx, id, err)
		return
	}
	// If the list does not fit into one frame, report the change set as incomplete. The client then reloads the
	// database, which is cheaper at that size anyway.
	if len(set.Blocks) > c.changesPerFrame() {
		set = store.ChangeSet{FromVersion: set.FromVersion, ToVersion: set.ToVersion}
	}
	c.send(ctx, &pb.ServerFrame{RequestId: id, Body: &pb.ServerFrame_Changes{Changes: &pb.Changes{
		FromVersion: set.FromVersion,
		ToVersion:   set.ToVersion,
		Blocks:      set.Blocks,
		Complete:    set.Complete,
	}}})
}

// changesPerFrame returns how many block indexes fit into one frame, at up to 10 bytes per varint.
func (c *connection) changesPerFrame() int {
	return max(1, int((c.s.opts.MaxFrameBytes-changesFrameOverhead)/10))
}

func (c *connection) commit(ctx context.Context, id uint64, req *pb.Commit) {
	db, err := c.openDB(req.GetDbId())
	if err != nil {
		c.fail(ctx, id, err)
		return
	}
	if len(req.GetCommitId()) != commitIDBytes {
		db.pending = nil
		c.fail(ctx, id, store.BadRequest("commit id must have %d bytes", commitIDBytes))
		return
	}

	pending := db.pending
	switch {
	case req.GetPart() == 0:
		// Part 0 always starts a new commit and discards any pending one.
		pending = &pendingCommit{header: &pb.Commit{
			CommitId:    req.GetCommitId(),
			LeaseEpoch:  req.GetLeaseEpoch(),
			BaseVersion: req.GetBaseVersion(),
			PageCount:   req.GetPageCount(),
		}}
		db.pending = pending
	case pending == nil || req.GetPart() != pending.nextPart || !continues(pending.header, req):
		// Discard the pending commit. Otherwise the parts after a rejected part could complete it, and the server
		// would apply a partial commit.
		db.pending = nil
		c.fail(ctx, id, store.BadRequest("part %d does not continue a pending commit", req.GetPart()))
		return
	}
	pending.nextPart++
	for _, block := range req.GetBlocks() {
		pending.size += uint64(len(block.GetData()))
		pending.blocks = append(pending.blocks, store.Block{Index: block.GetIndex(), Data: block.GetData()})
	}
	if pending.size > c.s.opts.MaxCommitBytes {
		db.pending = nil
		c.fail(ctx, id, errTooLarge)
		return
	}
	if req.GetMore() {
		// Only the last part of a commit gets a response.
		return
	}

	db.pending = nil
	now := c.s.opts.Now()
	version, err := c.s.opts.Store.Commit(ctx, store.CommitRequest{
		Key:         db.key,
		CommitID:    pending.header.GetCommitId(),
		LeaseEpoch:  pending.header.GetLeaseEpoch(),
		BaseVersion: pending.header.GetBaseVersion(),
		PageCount:   pending.header.GetPageCount(),
		Blocks:      pending.blocks,
		Now:         now,
		TTL:         c.s.opts.LeaseTTL,
	})
	if err != nil {
		c.fail(ctx, id, err)
		return
	}
	// The store extended the lease in the commit transaction.
	db.renewed = now
	c.send(ctx, &pb.ServerFrame{RequestId: id, Body: &pb.ServerFrame_CommitAck{CommitAck: &pb.CommitAck{
		CommitId: pending.header.GetCommitId(), Version: version,
	}}})
}

// continues reports whether a frame belongs to the pending commit: same commit ID, lease epoch, base version and
// page count.
func continues(header, frame *pb.Commit) bool {
	return bytes.Equal(header.GetCommitId(), frame.GetCommitId()) &&
		header.GetLeaseEpoch() == frame.GetLeaseEpoch() &&
		header.GetBaseVersion() == frame.GetBaseVersion() &&
		header.GetPageCount() == frame.GetPageCount()
}

func (c *connection) ping(ctx context.Context, id uint64, req *pb.Ping) {
	now := c.s.opts.Now()
	for dbID, db := range c.dbs {
		if !c.renewDue(db, now) {
			continue
		}
		if err := c.s.opts.Store.Renew(ctx, db.key, db.epoch, now, c.s.opts.LeaseTTL); err != nil {
			c.log.Info("lease not renewed", "db", dbID, "error", err)
			continue
		}
		db.renewed = now
	}
	c.send(ctx, &pb.ServerFrame{RequestId: id, Body: &pb.ServerFrame_Pong{Pong: &pb.Pong{
		ClientTimeMs: req.GetClientTimeMs(),
	}}})
}

func (c *connection) closeDB(ctx context.Context, id uint64, req *pb.CloseDb) {
	db, err := c.openDB(req.GetDbId())
	if err != nil {
		c.fail(ctx, id, err)
		return
	}
	if err := c.s.opts.Store.Release(ctx, db.key, req.GetLeaseEpoch()); err != nil {
		c.fail(ctx, id, err)
		return
	}
	delete(c.dbs, req.GetDbId())
	c.s.holders.remove(db.key, c)
	c.send(ctx, &pb.ServerFrame{RequestId: id, Body: &pb.ServerFrame_Ok{Ok: &pb.Ok{}}})
}

func (c *connection) openDB(dbID string) (*openDB, error) {
	db, ok := c.dbs[dbID]
	if !ok {
		return nil, store.BadRequest("database %q is not open on this connection", dbID)
	}
	return db, nil
}

func (c *connection) fail(ctx context.Context, id uint64, err error) {
	c.send(ctx, errorFrame(id, err, c.log))
}

func (c *connection) send(ctx context.Context, frame *pb.ServerFrame) {
	data, err := proto.Marshal(frame)
	if err != nil {
		c.log.Error("encoding a frame failed", "error", err)
		return
	}
	writeCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	if err := c.ws.Write(writeCtx, websocket.MessageBinary, data); err != nil {
		c.log.Debug("writing a frame failed", "error", err)
	}
}

func checkDBID(id string) error {
	if id == "" || len(id) > maxDBIDBytes || !utf8.ValidString(id) {
		return store.BadRequest("database id must be 1 to %d bytes of UTF-8", maxDBIDBytes)
	}
	return nil
}
