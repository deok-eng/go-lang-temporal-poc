package main

import (
	"log"

	orderworkflow "github.com/example/temporal-order-workflow"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// The worker is a long-running process that:
//   1. Connects to Temporal Server
//   2. Registers the workflow and activity functions it knows how to execute
//   3. Polls the task queue for tasks and runs them
//
// You can run multiple workers for the same task queue — Temporal will
// distribute tasks across them automatically (horizontal scaling).
func main() {
	// Create a Temporal client — this is the connection to Temporal Server.
	// By default it connects to localhost:7233 (the dev server address).
	c, err := client.Dial(client.Options{
		HostPort: client.DefaultHostPort, // "localhost:7233"
	})
	if err != nil {
		log.Fatalf("Failed to connect to Temporal Server: %v", err)
	}
	defer c.Close()

	// Create a worker that listens on our task queue.
	// The task queue name must match what the starter uses.
	w := worker.New(c, orderworkflow.TaskQueueName, worker.Options{})

	// Register the workflow function.
	// Temporal uses the function name as the workflow type identifier.
	w.RegisterWorkflow(orderworkflow.OrderWorkflow)

	// Register all activity functions.
	// Each activity is registered individually so Temporal knows which
	// function to call when it schedules an activity task.
	w.RegisterActivity(orderworkflow.ValidateOrder)
	w.RegisterActivity(orderworkflow.ChargePayment)
	w.RegisterActivity(orderworkflow.SendConfirmation)

	log.Printf("Worker started. Polling task queue: %q", orderworkflow.TaskQueueName)
	log.Println("Waiting for workflow tasks... (press Ctrl+C to stop)")

	// Run blocks until the worker is stopped (Ctrl+C or signal).
	// It polls Temporal Server for workflow and activity tasks in a loop.
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("Worker stopped with error: %v", err)
	}
}
