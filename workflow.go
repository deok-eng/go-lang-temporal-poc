package orderworkflow

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// TaskQueueName is the shared name used by both the worker and the starter.
// Think of it as the "channel" connecting them through Temporal Server.
const TaskQueueName = "order-task-queue"

// Order is the input data passed into the workflow when it starts.
type Order struct {
	OrderID    string
	CustomerID string
	Item       string
	Amount     float64
}

// OrderResult is what the workflow returns when it completes.
type OrderResult struct {
	OrderID string
	Status  string
	Message string
}

// OrderWorkflow is the workflow definition.
//
// A workflow function must:
//   - Accept workflow.Context as the first argument
//   - Be deterministic (no random numbers, no direct time.Now(), no goroutines outside workflow.Go)
//   - Use workflow.ExecuteActivity to call activities (never call them directly)
//
// Temporal replays this function from history on retries, so it must produce
// the same decisions given the same history — that's what "deterministic" means.
func OrderWorkflow(ctx workflow.Context, order Order) (OrderResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("OrderWorkflow started", "orderID", order.OrderID)

	// Activity options define retry behavior and timeouts for every activity
	// called within this context. You can override per-activity if needed.
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second, // max time a single activity attempt can run
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:    3,                // retry up to 3 times on failure
			InitialInterval:    time.Second,      // wait 1s before first retry
			BackoffCoefficient: 2.0,              // double the wait each retry
			MaximumInterval:    10 * time.Second, // cap the wait at 10s
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// ── Step 1: Validate the order ──────────────────────────────────────────
	// workflow.ExecuteActivity schedules the activity on the task queue.
	// The worker picks it up, runs it, and returns the result here.
	var validationResult string
	err := workflow.ExecuteActivity(ctx, ValidateOrder, order).Get(ctx, &validationResult)
	if err != nil {
		logger.Error("ValidateOrder failed", "error", err)
		return OrderResult{
			OrderID: order.OrderID,
			Status:  "FAILED",
			Message: "Order validation failed: " + err.Error(),
		}, err
	}
	logger.Info("Order validated", "result", validationResult)

	// ── Step 2: Charge payment ───────────────────────────────────────────────
	var paymentResult string
	err = workflow.ExecuteActivity(ctx, ChargePayment, order).Get(ctx, &paymentResult)
	if err != nil {
		logger.Error("ChargePayment failed", "error", err)
		return OrderResult{
			OrderID: order.OrderID,
			Status:  "FAILED",
			Message: "Payment failed: " + err.Error(),
		}, err
	}
	logger.Info("Payment charged", "result", paymentResult)

	// ── Step 3: Send confirmation ────────────────────────────────────────────
	var confirmationResult string
	err = workflow.ExecuteActivity(ctx, SendConfirmation, order).Get(ctx, &confirmationResult)
	if err != nil {
		logger.Error("SendConfirmation failed", "error", err)
		return OrderResult{
			OrderID: order.OrderID,
			Status:  "PARTIAL",
			Message: "Order processed but confirmation failed: " + err.Error(),
		}, err
	}
	logger.Info("Confirmation sent", "result", confirmationResult)

	// All steps succeeded — return a successful result
	return OrderResult{
		OrderID: order.OrderID,
		Status:  "COMPLETED",
		Message: "Order processed successfully. " + confirmationResult,
	}, nil
}
