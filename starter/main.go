package main

import (
	"context"
	"fmt"
	"log"

	orderworkflow "github.com/example/temporal-order-workflow"
	"go.temporal.io/sdk/client"
)

// The starter submits a new workflow execution to Temporal Server.
// It does NOT run the workflow itself — it just tells Temporal to run it.
// The worker (running separately) will pick it up and execute it.
func main() {
	// Connect to Temporal Server
	c, err := client.Dial(client.Options{
		HostPort: client.DefaultHostPort, // "localhost:7233"
	})
	if err != nil {
		log.Fatalf("Failed to connect to Temporal Server: %v", err)
	}
	defer c.Close()

	// Define the order we want to process
	order := orderworkflow.Order{
		OrderID:    "ORD-20260512-001",
		CustomerID: "CUST-42",
		Item:       "Mechanical Keyboard",
		Amount:     149.99,
	}

	// WorkflowOptions configure this specific execution.
	// WorkflowID is a unique identifier — if you submit the same ID twice,
	// Temporal will reject the second one (idempotency built-in).
	options := client.StartWorkflowOptions{
		ID:        "order-workflow-" + order.OrderID, // unique per order
		TaskQueue: orderworkflow.TaskQueueName,
	}

	// ExecuteWorkflow submits the workflow to Temporal Server.
	// This returns immediately — the workflow runs asynchronously on the worker.
	log.Printf("Submitting OrderWorkflow for order: %s", order.OrderID)
	we, err := c.ExecuteWorkflow(context.Background(), options, orderworkflow.OrderWorkflow, order)
	if err != nil {
		log.Fatalf("Failed to start workflow: %v", err)
	}

	log.Printf("Workflow started — WorkflowID: %s, RunID: %s", we.GetID(), we.GetRunID())
	log.Println("Waiting for workflow to complete...")

	// .Get() blocks until the workflow finishes and returns the result.
	// In production you might not wait here — you'd poll or use signals instead.
	var result orderworkflow.OrderResult
	if err := we.Get(context.Background(), &result); err != nil {
		log.Fatalf("Workflow failed: %v", err)
	}

	// Print the final result
	fmt.Println("\n── Workflow Result ──────────────────────────")
	fmt.Printf("  Order ID : %s\n", result.OrderID)
	fmt.Printf("  Status   : %s\n", result.Status)
	fmt.Printf("  Message  : %s\n", result.Message)
	fmt.Println("─────────────────────────────────────────────")
}
