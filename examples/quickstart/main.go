// Command quickstart shows the ~10-line wire-in for ab0t-events-go using the
// in-process worker pool and in-memory stores. Run: go run ./examples/quickstart
package main

import (
	"context"
	"fmt"

	"github.com/ab0t-com/ab0t-events-go/adapters/hmacsign"
	"github.com/ab0t-com/ab0t-events-go/adapters/memory"
	"github.com/ab0t-com/ab0t-events-go/events"
	"github.com/ab0t-com/ab0t-events-go/webhooks"
)

func main() {
	ctx := context.Background()

	// --- wire-in: the whole thing (facade with production-safe defaults) ---
	sys, err := webhooks.New(ctx, webhooks.Options{
		Store: memory.NewStore(), // swap for dynamo.NewStore(ddb, "subscriptions")
		Log:   memory.NewLog(),   // swap for dynamo.NewLog(ddb, "deliveries")
	})
	if err != nil {
		panic(err)
	}
	defer sys.Close() // stops the worker pool; an interrupted attempt is not recorded — call Recover at start
	// --- end wire-in ---

	// An org registers an endpoint (secret shown to them once).
	secret, _ := hmacsign.GenerateSecret()
	sub, _ := sys.Subscribe(ctx, events.NewSubscription{
		OrgID:      "org_123",
		EventTypes: []string{"drive.*"}, // prefix wildcard
		URL:        "https://example.com/webhooks/ab0t",
		Secret:     secret,
	})
	fmt.Println("subscribed:", sub.ID)

	// Emit a domain event — id, timestamp and JSON marshalling are handled.
	if err := sys.EmitNew(ctx, "drive.file.created", "org_123", map[string]any{
		"fileId": "f_1", "name": "q3.pdf",
	}); err != nil {
		panic(err)
	}
	fmt.Println("event emitted; the worker pool will sign, deliver, retry and log it")
}
