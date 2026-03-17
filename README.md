```
  _____   ______  _____    _____  _    _  __  __  ______  _____
 |  __ \ |  ____||  __ \  / ____|| |  | ||  \/  ||  ____||  __ \
 | |__) || |__   | |  | || (___  | |  | || \  / || |__   | |__) |
 |  _  / |  __|  | |  | | \___ \ | |  | || |\/| ||  __|  |  _  /
 | | \ \ | |____ | |__| | ____) || |__| || |  | || |____ | | \ \
 |_|  \_\|______||_____/ |_____/  \____/ |_|  |_||______||_|  \_\
```

## Description

Redsumer is a Go library that abstracts Redis Stream consumption. It provides horizontal scalability, adaptive protection against Redis overload when the queue is idle or stalled, priority for new messages when the PEL is not progressing, and a simple contract: the library handles infrastructure, the user handles business logic.

Built on top of [valkey-go](https://github.com/valkey-io/valkey-go).

## Features

- **Adaptive ratio** — interleaves new messages and PEL processing at a configurable ratio; automatically reduces PEL attempt frequency when the PEL is stalled
- **Adaptive backoff** — exponential-style wait when the queue is completely empty, configurable via a slice of durations
- **PEL stall detection** — compares PEL size across full XAUTOCLAIM traversals and adjusts both ratio and wait accordingly
- **Blocking XREADGROUP** — efficient wait for new messages without busy-polling
- **Auto group creation and recreation** — creates the consumer group on startup; recreates it automatically if deleted at runtime
- **Context-aware sleeps** — all waits respect context cancellation for clean shutdown
- **Horizontal scaling** — multiple instances under the same consumer group, Redis guarantees each message is delivered to exactly one instance
- **Observability** — `Stats()` exposes current adaptive state indices

## Installation

```bash
go get github.com/enerBit/redsumer/v4
```

## Consumer usage

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"

    "github.com/enerBit/redsumer/v4/pkg/client"
    "github.com/enerBit/redsumer/v4/pkg/consumer"
)

func main() {
    ctx := context.Background()

    c := &consumer.Consumer{
        // Redis connection
        Client: &client.ClientArgs{
            Host: "localhost",
            Port: "6379",
        },

        // Stream identity
        StreamName:   "my-stream",
        GroupName:    "my-group",
        ConsumerName: consumer.DefaultConsumerName(), // hostname-pid; must be unique per instance

        // How long to wait for new messages before triggering PEL (ms)
        BlockMs: 2000,

        // XAUTOCLAIM settings
        ClaimMinIdleMs: 30000, // claim messages idle for > 30s
        ClaimBatch:     10,

        // XREADGROUP batch size
        BatchSize: 10,

        // Idle threshold for StillMine check (ms)
        IdleStillMine: 5000,

        // Retries while waiting for the stream to exist (seconds between each)
        Tries: []int{1, 2, 5, 10},

        // Adaptive slices — index 0 is the healthy-state value;
        // index advances when the PEL is stalled, resets when it progresses.
        RatioSlice:   []int{5, 10, 20, 50},   // new messages per PEL batch
        PelWaitSlice: []int{0, 1, 5, 30},     // seconds before each PEL attempt
        BackoffSlice: []int{1, 2, 5, 10, 30}, // seconds when queue is completely empty
    }

    if err := c.InitConsumer(ctx); err != nil {
        log.Fatal(err)
    }

    for {
        messages, err := c.Consume(ctx)
        if err != nil {
            log.Println("consume error:", err)
            continue
        }

        for _, msg := range messages {
            // Optional: verify the message is still assigned to this consumer
            if ok, _ := c.StillMine(ctx, msg.ID); !ok {
                fmt.Println("message reclaimed by another consumer:", msg.ID)
                continue
            }

            fmt.Println("processing:", msg.ID, msg.Values)

            // Acknowledge when processing is complete
            if err := c.AcknowledgeMessage(ctx, msg.ID); err != nil {
                log.Println("ack error:", err)
            }
        }

        // Optional: inspect adaptive state
        s := c.Stats()
        fmt.Printf("ratioIdx=%d pelWaitIdx=%d backoffIdx=%d prevPelSize=%d\n",
            s.RatioIdx, s.PelWaitIdx, s.BackoffIdx, s.PrevPelSize)
    }
}
```

### Graceful shutdown

Pass a cancellable context. All sleeps (backoff, PEL wait) will unblock immediately when the context is cancelled.

```go
ctx, cancel := context.WithCancel(context.Background())

// cancel on SIGTERM / SIGINT
go func() {
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt, syscall.SIGTERM)
    <-c
    cancel()
}()

for {
    msgs, err := consumer.Consume(ctx)
    if err != nil {
        if errors.Is(err, context.Canceled) {
            break
        }
        log.Println(err)
    }
    // ...
}
```

## Producer usage

```go
package main

import (
    "context"
    "log"

    "github.com/enerBit/redsumer/v4/pkg/client"
    "github.com/enerBit/redsumer/v4/pkg/producer"
)

func main() {
    ctx := context.Background()

    p := &producer.Producer{
        Client: &client.ClientArgs{
            Host: "localhost",
            Port: "6379",
        },
    }

    if err := p.Client.InitClient(ctx); err != nil {
        log.Fatal(err)
    }

    err := p.Produce(ctx, map[string]string{
        "event": "order.created",
        "id":    "42",
    }, "my-stream")
    if err != nil {
        log.Fatal(err)
    }
}
```

## Configuration reference

| Field | Type | Description |
|---|---|---|
| `StreamName` | `string` | Redis stream name |
| `GroupName` | `string` | Consumer group name |
| `ConsumerName` | `string` | Unique consumer name per instance (use `DefaultConsumerName()`) |
| `BlockMs` | `int64` | XREADGROUP blocking wait (ms) |
| `ClaimMinIdleMs` | `int64` | Minimum idle time before XAUTOCLAIM reclaims a message (ms) |
| `ClaimBatch` | `int64` | Messages per XAUTOCLAIM batch |
| `BatchSize` | `int64` | Messages per XREADGROUP batch |
| `IdleStillMine` | `int64` | Idle threshold for `StillMine` check (ms) |
| `Tries` | `[]int` | Seconds between retries while waiting for stream existence |
| `RatioSlice` | `[]int` | New messages per PEL batch. Advances when PEL stalls. e.g. `[5, 10, 20, 50]` |
| `PelWaitSlice` | `[]int` | Seconds before each PEL attempt. Advances when PEL stalls. e.g. `[0, 1, 5, 30]` |
| `BackoffSlice` | `[]int` | Seconds when queue is completely empty. e.g. `[1, 2, 5, 10, 30]` |

All slice fields must have at least one element. `BatchSize` and `ClaimBatch` must be > 0. Validation runs in `InitConsumer` before any network connection is attempted.

## Adaptive loop behaviour

Each call to `Consume()` represents one iteration. The caller drives the outer `for` loop.

**Phase 1 — new messages (`XREADGROUP BLOCK`)**
Reads up to `BatchSize` new messages, blocking for up to `BlockMs` ms. Accumulates a counter. When the counter reaches `RatioSlice[ratioIdx]`, the PEL phase runs and the counter resets. If no new messages arrive, the PEL phase runs immediately.

**Phase 2 — PEL (`XAUTOCLAIM`)**
Claims one batch of up to `ClaimBatch` messages that have been idle for at least `ClaimMinIdleMs` ms. A `PelWaitSlice[pelWaitIdx]` second wait is applied before each attempt.

When the cursor wraps back to `0-0` (full PEL traversal complete), the current PEL size is compared with the previous traversal:
- **PEL size changed** → `ratioIdx` and `pelWaitIdx` reset to 0 (PEL is progressing)
- **PEL size unchanged** → both indices advance (PEL is stalled; reduce frequency)

**Phase 3 — backoff**
Applies only when both phases return empty. Sleeps for `BackoffSlice[backoffIdx]` seconds and advances the index. Resets when messages arrive.

## Horizontal scaling

Multiple instances under the same `GroupName` — Redis delivers each message to exactly one instance. The `ConsumerName` must be unique per instance; `DefaultConsumerName()` generates `hostname-pid`.

## What the library does NOT do

- No business logic on messages
- No retry decision — unacked messages stay in the PEL and are reclaimed by XAUTOCLAIM after `ClaimMinIdleMs`
- No internal concurrency — sequential batch processing; parallelism is the user's responsibility
- No DLQ or dead-letter stream
- No state persistence between restarts
- No circuit breaker

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change. Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)
