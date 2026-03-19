package consumer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/enerBit/redsumer/v4/pkg/client"
	errors_custom "github.com/enerBit/redsumer/v4/pkg/errors"
	"github.com/valkey-io/valkey-go"
)

const (
	consumer_NEVER_DELIVERED_TO_OTHER_CONSUMERS_SO_FAR = ">"
	consumer_INITIAL_STREAM_ID                         = "0-0"
	consumer_NOGROUP                                   = "NOGROUP No such key"
)

// Consumer holds configuration and internal adaptive state for consuming a Redis Stream.
// All slice fields must have at least one element; BatchSize and ClaimBatch must be > 0.
type Consumer struct {
	// --- User configuration ---
	Client       *client.ClientArgs
	StreamName   string
	GroupName    string
	ConsumerName string
	Tries        []int // seconds between retries while waiting for stream existence

	BlockMs        int64 // XREADGROUP BLOCK timeout (ms)
	ClaimMinIdleMs int64 // XAUTOCLAIM min idle time (ms)
	ClaimBatch     int64 // XAUTOCLAIM batch size
	BatchSize      int64 // XREADGROUP batch size
	IdleStillMine  int64 // XPENDING idle threshold used by StillMine

	// Adaptive slices (length >= 1 enforced in InitConsumer)
	RatioSlice   []int // new messages per PEL batch; index advances when PEL stalls. e.g. [5,10,20,50]
	PelWaitSlice []int // seconds before each PEL attempt; advances when stalled. e.g. [0,1,5,30]
	BackoffSlice []int // seconds when queue completely empty. e.g. [1,2,5,10,30]

	// --- Internal adaptive state ---
	nextIdAutoClaim      string
	newMsgCounter        int
	ratioIdx             int
	pelWaitIdx           int
	backoffIdx           int
	prevPelSize          int64 // -1 = no full traversal recorded yet
	pelTraversalComplete bool
}

// ConsumerStats is a snapshot of the consumer's internal adaptive state for observability.
type ConsumerStats struct {
	RatioIdx    int
	PelWaitIdx  int
	BackoffIdx  int
	PrevPelSize int64
}

// Stats returns a snapshot of the current adaptive state indices.
func (c *Consumer) Stats() ConsumerStats {
	return ConsumerStats{
		RatioIdx:    c.ratioIdx,
		PelWaitIdx:  c.pelWaitIdx,
		BackoffIdx:  c.backoffIdx,
		PrevPelSize: c.prevPelSize,
	}
}

// DefaultConsumerName returns a unique consumer name built from hostname and PID.
// Recommended for horizontal scaling (each instance must have a unique name).
func DefaultConsumerName() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	return hostname + "-" + strconv.Itoa(os.Getpid())
}

// validateConfig checks all required configuration fields before connecting.
func validateConfig(c *Consumer) error {
	if c.Client == nil {
		return fmt.Errorf("%w: client must not be nil", errors_custom.ErrInvalidConfig)
	}
	if strings.TrimSpace(c.StreamName) == "" {
		return fmt.Errorf("%w: stream_name must not be empty", errors_custom.ErrInvalidConfig)
	}
	if strings.TrimSpace(c.GroupName) == "" {
		return fmt.Errorf("%w: group_name must not be empty", errors_custom.ErrInvalidConfig)
	}
	if strings.TrimSpace(c.ConsumerName) == "" {
		return fmt.Errorf("%w: consumer_name must not be empty", errors_custom.ErrInvalidConfig)
	}
	if len(c.RatioSlice) == 0 {
		return fmt.Errorf("%w: ratio_slice must not be empty", errors_custom.ErrInvalidConfig)
	}
	if len(c.PelWaitSlice) == 0 {
		return fmt.Errorf("%w: pel_wait_slice must not be empty", errors_custom.ErrInvalidConfig)
	}
	if len(c.BackoffSlice) == 0 {
		return fmt.Errorf("%w: backoff_slice must not be empty", errors_custom.ErrInvalidConfig)
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("%w: batch_size must be > 0", errors_custom.ErrInvalidConfig)
	}
	if c.ClaimBatch <= 0 {
		return fmt.Errorf("%w: claim_batch must be > 0", errors_custom.ErrInvalidConfig)
	}
	return nil
}

// InitConsumer validates configuration, initializes the Redis client, resets internal state,
// and creates the consumer group if it does not already exist.
func (c *Consumer) InitConsumer(ctx context.Context) error {
	if err := validateConfig(c); err != nil {
		return err
	}

	if err := c.Client.InitClient(ctx); err != nil {
		return err
	}

	c.nextIdAutoClaim = consumer_INITIAL_STREAM_ID
	c.newMsgCounter = 0
	c.ratioIdx = 0
	c.pelWaitIdx = 0
	c.backoffIdx = 0
	c.prevPelSize = -1
	c.pelTraversalComplete = false

	return c.initGroup(ctx)
}

// exist checks whether a stream key exists in Redis.
func (c *Consumer) exist(ctx context.Context, key string) error {
	cmd := c.Client.Instance.B().Exists().Key(key).Build()
	e, err := c.Client.Instance.Do(ctx, cmd).ToInt64()
	if err != nil {
		return err
	}
	if e != 1 {
		return errors_custom.ErrKeyNotFound
	}
	return nil
}

// waitForStream retries existence checks using c.Tries delays (in seconds).
// Returns ErrStreamNotFound if the stream never appears.
func (c *Consumer) waitForStream(ctx context.Context) error {
	for _, waitTime := range c.Tries {
		err := c.exist(ctx, c.StreamName)
		if err == nil {
			return nil
		}
		// Only retry when the key truly does not exist.
		if !errors.Is(err, errors_custom.ErrKeyNotFound) {
			return err
		}
		wait := time.Second * time.Duration(waitTime)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
			// continue to next retry
		}
	}
	return errors_custom.ErrStreamNotFound
}

// initGroup waits for the stream and creates the consumer group.
// A BUSYGROUP error (group already exists) is treated as success.
func (c *Consumer) initGroup(ctx context.Context) error {
	if err := c.waitForStream(ctx); err != nil {
		return err
	}

	cmd := c.Client.Instance.B().XgroupCreate().Key(c.StreamName).Group(c.GroupName).Id(consumer_INITIAL_STREAM_ID).Build()
	err := c.Client.Instance.Do(ctx, cmd).Error()
	if err != nil {
		var errV *valkey.ValkeyError
		if errors.As(err, &errV) {
			if errV.IsBusyGroup() {
				return nil
			}
			return errV
		}
		return err
	}
	return nil
}

// validateError recreates the consumer group if err signals NOGROUP (group was deleted at runtime).
// Returns nil so the caller can retry; returns the original error otherwise.
func (c *Consumer) validateError(ctx context.Context, err error) error {
	if strings.Contains(err.Error(), consumer_NOGROUP) {
		return c.initGroup(ctx)
	}
	return err
}

// StillMine reports whether the message identified by messageID is still in the PEL
// for this consumer (idle for at least IdleStillMine ms).
func (c *Consumer) StillMine(ctx context.Context, messageID string) (bool, error) {
	cmd := c.Client.Instance.B().Xpending().Key(c.StreamName).Group(c.GroupName).Idle(c.IdleStillMine).Start(messageID).End(messageID).Count(1).Consumer(c.ConsumerName).Build()
	v, err := c.Client.Instance.Do(ctx, cmd).ToArray()
	if err != nil {
		return false, err
	}
	return len(v) != 0, nil
}

// AcknowledgeMessage sends XACK for a single message ID.
func (c *Consumer) AcknowledgeMessage(ctx context.Context, messageID string) error {
	cmd := c.Client.Instance.B().Xack().Key(c.StreamName).Group(c.GroupName).Id(messageID).Build()
	v, err := c.Client.Instance.Do(ctx, cmd).AsBool()
	if err != nil {
		return err
	}
	if !v {
		return errors_custom.ErrNoAckedMessage
	}
	return nil
}

// newMessages fetches up to BatchSize new (never-delivered) messages from the stream,
// blocking for up to BlockMs milliseconds.
// Returns nil, nil when the block timeout expires with no messages.
func (c *Consumer) newMessages(ctx context.Context) ([]valkey.XRangeEntry, error) {
	cmd := c.Client.Instance.B().Xreadgroup().Group(c.GroupName, c.ConsumerName).Count(c.BatchSize).Block(c.BlockMs).Streams().Key(c.StreamName).Id(consumer_NEVER_DELIVERED_TO_OTHER_CONSUMERS_SO_FAR).Build()
	v, err := c.Client.Instance.Do(ctx, cmd).AsXRead()
	if err != nil {
		var errV *valkey.ValkeyError
		if errors.As(err, &errV) {
			if errV.IsNil() {
				return nil, nil // block timeout with no messages is not an error
			}
			return nil, errV
		}
		return nil, err
	}
	return v[c.StreamName], nil
}

// autoClaimMessages runs one XAUTOCLAIM batch starting from the current cursor.
// Sets pelTraversalComplete = true when the returned cursor resets to "0-0".
func (c *Consumer) autoClaimMessages(ctx context.Context) ([]valkey.XRangeEntry, error) {
	c.pelTraversalComplete = false

	cmd := c.Client.Instance.B().Xautoclaim().Key(c.StreamName).Group(c.GroupName).Consumer(c.ConsumerName).MinIdleTime(strconv.FormatInt(c.ClaimMinIdleMs, 10)).Start(c.nextIdAutoClaim).Count(c.ClaimBatch).Build()
	v, err := c.Client.Instance.Do(ctx, cmd).ToArray()
	if err != nil {
		return nil, err
	}

	nextID, err := v[0].ToString()
	if err != nil {
		return nil, err
	}
	c.nextIdAutoClaim = nextID
	if c.nextIdAutoClaim == consumer_INITIAL_STREAM_ID {
		c.pelTraversalComplete = true
	}

	entries, err := v[1].AsXRange()
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// pelSize returns the total number of pending messages in the group via XPENDING summary form.
func (c *Consumer) pelSize(ctx context.Context) (int64, error) {
	cmd := c.Client.Instance.B().Xpending().Key(c.StreamName).Group(c.GroupName).Build()
	v, err := c.Client.Instance.Do(ctx, cmd).ToArray()
	if err != nil {
		return 0, err
	}
	if len(v) == 0 {
		return 0, nil
	}
	return v[0].ToInt64()
}

// contextSleep sleeps for d, returning ctx.Err() immediately if the context is cancelled.
func contextSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// advanceIdx returns idx+1 capped at maxIdx (stays at maxIdx indefinitely).
func advanceIdx(idx, maxIdx int) int {
	if idx < maxIdx {
		return idx + 1
	}
	return maxIdx
}

// Consume runs one iteration of the adaptive consumption loop and returns a batch of messages.
//
// Phase 1 — new messages: XREADGROUP BLOCK up to BlockMs.
// Phase 2 — PEL (XAUTOCLAIM): triggered when the ratio counter is met or no new messages arrived.
//
//	After a full PEL traversal the PEL size is compared with the previous traversal:
//	  - changed → reset ratioIdx and pelWaitIdx to 0 (PEL is progressing)
//	  - unchanged → advance both indices (PEL is stalled, reduce attempt frequency)
//
// Phase 3 — backoff: applied only when both phases return empty; index advances each call.
//
// New messages take priority when both phases return results in the same iteration.
// The caller drives the outer loop.
func (c *Consumer) Consume(ctx context.Context) ([]valkey.XRangeEntry, error) {
retry:
	// ── Phase 1: new messages ─────────────────────────────────────────────
	msgs, err := c.newMessages(ctx)
	if err != nil {
		if err = c.validateError(ctx, err); err == nil {
			goto retry
		}
		return nil, err
	}

	if len(msgs) > 0 {
		c.newMsgCounter += len(msgs)
		c.backoffIdx = 0 // messages are flowing; reset backoff

		if c.newMsgCounter < c.RatioSlice[c.ratioIdx] {
			// Ratio threshold not yet met; skip PEL this call.
			return msgs, nil
		}
	}

	// ── Phase 2: PEL ──────────────────────────────────────────────────────
	// Reached when: ratio met OR no new messages arrived.
	c.newMsgCounter = 0

	if err := contextSleep(ctx, time.Duration(c.PelWaitSlice[c.pelWaitIdx])*time.Second); err != nil {
		return nil, err
	}

	pelMsgs, err := c.autoClaimMessages(ctx)
	if err != nil {
		if err = c.validateError(ctx, err); err == nil {
			goto retry
		}
		return nil, err
	}

	if c.pelTraversalComplete {
		currentSize, sizeErr := c.pelSize(ctx)
		if sizeErr != nil {
			if sizeErr = c.validateError(ctx, sizeErr); sizeErr == nil {
				goto retry
			}
			return nil, sizeErr
		}
		if currentSize != c.prevPelSize {
			// PEL size changed (progressed or grew) → reset protection indices.
			c.ratioIdx = 0
			c.pelWaitIdx = 0
		} else {
			// PEL unchanged → advance both indices to reduce attempt frequency.
			c.ratioIdx = advanceIdx(c.ratioIdx, len(c.RatioSlice)-1)
			c.pelWaitIdx = advanceIdx(c.pelWaitIdx, len(c.PelWaitSlice)-1)
		}
		c.prevPelSize = currentSize
	}

	if len(msgs) > 0 && len(pelMsgs) > 0 {
		// Return new messages first, then reclaimed PEL messages.
		combined := append(msgs, pelMsgs...)
		c.backoffIdx = 0
		return combined, nil
	}

	if len(msgs) > 0 {
		// New messages take priority; PEL cursor advanced as a side effect.
		return msgs, nil
	}
	if len(pelMsgs) > 0 {
		c.backoffIdx = 0
		return pelMsgs, nil
	}

	// ── Phase 3: backoff (both phases empty) ──────────────────────────────
	if err := contextSleep(ctx, time.Duration(c.BackoffSlice[c.backoffIdx])*time.Second); err != nil {
		return nil, err
	}
	c.backoffIdx = advanceIdx(c.backoffIdx, len(c.BackoffSlice)-1)
	return nil, nil
}
