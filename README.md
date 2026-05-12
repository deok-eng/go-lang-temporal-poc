# Temporal Workflow in Go — Complete Guide

> A simple, working example of a Temporal workflow built in Go.  
> We model an **order processing system** — validate an order, charge payment, send confirmation.

---

## Table of Contents

1. [What is Temporal?](#1-what-is-temporal)
2. [What Problem Does It Solve?](#2-what-problem-does-it-solve)
3. [What We Built](#3-what-we-built)
4. [Key Concepts Explained](#4-key-concepts-explained)
5. [Who is the Real Orchestrator?](#5-who-is-the-real-orchestrator)
6. [What Exactly is a Worker? EC2? Thread? Process?](#6-what-exactly-is-a-worker-ec2-thread-process)
7. [Understanding Activities — 3 Functions, 3 Tasks](#7-understanding-activities--3-functions-3-tasks)
8. [Project Structure](#8-project-structure)
9. [Architecture Diagram](#9-architecture-diagram)
10. [How the Code Works — File by File](#10-how-the-code-works--file-by-file)
11. [The Full Execution Flow](#11-the-full-execution-flow)
12. [Prerequisites](#12-prerequisites)
13. [Running the Project Step by Step](#13-running-the-project-step-by-step)
14. [Monitoring with CLI](#14-monitoring-with-cli)
15. [Monitoring with the Web UI](#15-monitoring-with-the-web-ui)
16. [What the Logs Tell You](#16-what-the-logs-tell-you)
17. [What Happens When Things Fail](#17-what-happens-when-things-fail)

---

## 1. What is Temporal?

Temporal is a **workflow orchestration platform**. It lets you write long-running, multi-step business processes as plain Go code — and it guarantees those processes run to completion even if your servers crash, the network drops, or an external API is down.

Think of it as a durable execution engine. You write the logic. Temporal handles:

- Retrying failed steps automatically
- Persisting state so nothing is lost on crash
- Scheduling timeouts
- Giving you full visibility into every step via a Web UI

---

## 2. What Problem Does It Solve?

Imagine you need to process an order in 3 steps:

```
1. Validate the order
2. Charge the customer's card
3. Send a confirmation email
```

Without Temporal, you'd write something like:

```go
err := validateOrder(order)
if err != nil { /* handle */ }

err = chargePayment(order)
if err != nil { /* handle, but did step 1 already run? */ }

err = sendConfirmation(order)
if err != nil { /* handle, but did step 2 already charge? */ }
```

Problems with this approach:
- If the process crashes between step 2 and 3, you've charged the card but never sent the email
- You have to write retry logic yourself in every function
- There's no visibility into what step failed or why
- Restarting means re-running everything from scratch

**With Temporal**, each step is a checkpoint. If the process crashes after step 2, Temporal resumes from step 3 — not from the beginning. The charge doesn't happen twice.

---

## 3. What We Built

A working Go application that processes an order through 3 activities:

| Step | Activity | What it does |
|------|----------|--------------|
| 1 | `ValidateOrder` | Checks order ID, amount, and item are valid |
| 2 | `ChargePayment` | Simulates charging the customer |
| 3 | `SendConfirmation` | Simulates sending a confirmation email |

The workflow runs these 3 steps in sequence. Each step can fail and be retried independently. The whole thing is observable in real time via the Temporal Web UI.

---

## 4. Key Concepts Explained

### Workflow
A workflow is a **Go function that orchestrates the steps**. It defines what happens and in what order. It does NOT do the actual work itself — it delegates to activities.

```go
func OrderWorkflow(ctx workflow.Context, order Order) (OrderResult, error) {
    workflow.ExecuteActivity(ctx, ValidateOrder, order)
    workflow.ExecuteActivity(ctx, ChargePayment, order)
    workflow.ExecuteActivity(ctx, SendConfirmation, order)
}
```

One critical rule: **workflows must be deterministic**. No `time.Now()`, no random numbers, no direct goroutines. This is because Temporal replays the workflow function from its history when recovering from a crash — it needs to produce the same decisions every time.

### Activity
An activity is a **plain Go function that does the actual work**. It can call databases, APIs, send emails — anything. Activities are allowed to have side effects and are NOT required to be deterministic.

```go
func ValidateOrder(ctx context.Context, order Order) (string, error) {
    // call a DB, check inventory, whatever you need
}
```

If an activity fails, Temporal retries it automatically based on the retry policy you configure.

### Worker
The worker is a **long-running Go process** that:
1. Connects to Temporal Server
2. Registers which workflows and activities it can execute
3. Polls the task queue in a loop, picks up tasks, runs them, reports results back

The worker is where your code actually runs. Temporal Server never runs your code — it only schedules and tracks it.

### Starter
The starter is a **short-lived Go program** that submits a new workflow execution to Temporal Server. It's like placing an order — it hands the request to Temporal and optionally waits for the result.

The starter does NOT execute the workflow. It just says "please run this workflow with this input."

### Temporal Server
The server is the **brain of the system**. It:
- Receives workflow execution requests from starters
- Stores the full history of every workflow execution durably
- Puts tasks on the task queue for workers to pick up
- Tracks retries, timeouts, and state
- Serves the Web UI for monitoring

In development, you run it locally with `temporal server start-dev`. In production, it runs as a cluster (or you use Temporal Cloud).

### Task Queue
A task queue is a **named channel** that connects starters and workers through the server. The starter says "run this on queue X", the worker says "I handle queue X". Temporal routes tasks between them.

In this project the task queue is named `order-task-queue`.

---

## 5. Who is the Real Orchestrator?

This is the most important question to understand about Temporal. The answer is: **it's a split responsibility between Temporal Server and the Worker**. They each own a different half of orchestration.

---

### Temporal Server = Orchestration Authority

Temporal Server is the **decision maker and state keeper**. It:

- Decides what needs to happen next
- Schedules tasks onto the queue
- Manages retries when activities fail
- Enforces timeouts
- Stores the complete history of every execution durably
- Is the single source of truth for workflow state

But here is the critical thing — **Temporal Server never runs your code**. It has no idea what `ValidateOrder` does. It just knows there is a task of type `ValidateOrder` that needs to run on `order-task-queue`.

---

### Worker = Orchestration Executor

The Worker is where your `OrderWorkflow` function **physically executes**. So in a literal sense, the code that says "call step 1, then step 2, then step 3" runs inside the worker process.

But the worker owns no state. Every decision it makes is immediately recorded back to Temporal Server as an event. If the worker crashes, Temporal Server replays the history into a new worker and the workflow continues from where it left off.

---

### The Court Analogy

| Role | Court equivalent |
|------|-----------------|
| Temporal Server | Judge + court record keeper |
| Worker | Lawyer who argues the case (runs the logic) |
| Workflow function | The legal strategy — what to do and in what order |
| Activities | The actual actions taken — filing documents, making calls |
| Starter | The plaintiff who files the case |

The lawyer (worker) runs the strategy (workflow function). But every decision is recorded by the court (Temporal Server). If the lawyer disappears mid-case, a new lawyer picks up the exact same case from the court records and continues.

---

### Visualised

```
┌─────────────────────────────────────────────────────────────────┐
│                     Temporal Server                              │
│                                                                   │
│   "What needs to happen next?"                                    │
│   Stores history, schedules tasks, manages retries & timeouts    │
│                                                                   │
│              ORCHESTRATION AUTHORITY                              │
│              (holds state, makes scheduling decisions)            │
└──────────────────────────┬──────────────────────────────────────┘
                           │ sends task: "run OrderWorkflow"
                           │ sends task: "run ValidateOrder"
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Worker                                    │
│                                                                   │
│   Runs OrderWorkflow() function                                   │
│   "step 1 → step 2 → step 3"                                     │
│   Reports every decision back to Temporal Server                  │
│                                                                   │
│              ORCHESTRATION EXECUTOR                               │
│              (runs the logic, owns no state)                      │
└─────────────────────────────────────────────────────────────────┘
```

---

### The Worker is Stateless — This is the Key Insight

Watch what happens when a worker crashes mid-workflow:

```
Worker runs OrderWorkflow...
  ✅ ValidateOrder  completed → event written to Temporal Server
  ✅ ChargePayment  completed → event written to Temporal Server
  💥 Worker crashes here

New worker starts up...
  Temporal Server: "here is the history — 2 activities already completed"
  Worker replays OrderWorkflow from the top
    → ValidateOrder:  already done, skip it (return cached result from history)
    → ChargePayment:  already done, skip it (return cached result from history)
    → SendConfirmation: NOT done, run it fresh
```

The workflow function re-executes on the new worker, but Temporal feeds it the recorded results for already-completed steps. It fast-forwards to exactly where it left off. **The worker is replaceable. The state lives in Temporal Server.**

---

### Summary: who owns what

| Question | Answer |
|----------|--------|
| Who holds state and makes scheduling decisions? | **Temporal Server** |
| Where does the workflow logic actually execute? | **Worker** |
| Who defines what the orchestration logic is? | **You, in `workflow.go`** |
| Who triggers the whole thing? | **Starter** |

The cleanest one-liner: **Temporal Server is the orchestration engine, the Worker is the orchestration runtime, your workflow function is the orchestration logic.**

---

## 6. What Exactly is a Worker? EC2? Thread? Process?

The Worker is a **process** — think of it as an EC2 instance or a Kubernetes pod, not a thread.

---

### The three layers

```
┌──────────────────────────────────────────────────────────────┐
│  Infrastructure Layer  (EC2 / ECS Task / K8s Pod / Laptop)   │
│                                                               │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  OS Process  (your compiled Go binary)                  │  │
│  │  started with: go run worker/main.go                    │  │
│  │                                                         │  │
│  │  ┌──────────────────────────────────────────────────┐  │  │
│  │  │  Temporal Worker SDK (internal goroutines)        │  │  │
│  │  │                                                   │  │  │
│  │  │  Goroutine 1 ──► polls for workflow tasks         │  │  │
│  │  │                   runs OrderWorkflow()            │  │  │
│  │  │                                                   │  │  │
│  │  │  Goroutine 2 ──► polls for activity tasks         │  │  │
│  │  │                   runs ValidateOrder()            │  │  │
│  │  │                                                   │  │  │
│  │  │  Goroutine 3 ──► polls for activity tasks         │  │  │
│  │  │                   runs ChargePayment()            │  │  │
│  │  │                                                   │  │  │
│  │  │  Goroutine N ──► polls for activity tasks         │  │  │
│  │  │                   runs SendConfirmation()         │  │  │
│  │  └──────────────────────────────────────────────────┘  │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

| Layer | What it is |
|-------|-----------|
| EC2 / Pod | The machine the worker runs on |
| OS Process | The worker binary (`go run worker/main.go`) |
| Goroutines | Internal threads the SDK uses to poll and execute concurrently |

---

### The polling model — workers pull, never receive

Workers do not receive tasks pushed to them. They **long-poll** Temporal Server:

```
Worker goroutine loop:
  ┌─────────────────────────────────────────────────────────┐
  │                                                         │
  │  1. Ask Temporal Server:                                │
  │     "Any tasks on order-task-queue?"                    │
  │                                                         │
  │  2. Temporal holds the connection open (long-poll)      │
  │     until a task is available — could be ms or hours    │
  │                                                         │
  │  3. Task arrives:                                       │
  │     "Run ValidateOrder with this input"                 │
  │                                                         │
  │  4. Worker executes ValidateOrder()                     │
  │                                                         │
  │  5. Worker reports result back to Temporal Server       │
  │                                                         │
  │  6. Go back to step 1                                   │
  │                                                         │
  └─────────────────────────────────────────────────────────┘
```

This polling model is why workers work behind firewalls, in private subnets, even on your laptop. The worker always initiates the connection **outbound** to Temporal Server. Temporal Server never needs to reach into your network to push tasks.

---

### How workers scale horizontally

Because a worker is just a process, you scale it exactly like any other service — run more instances:

```
                       Temporal Server
                       localhost:7233
                            │
           ┌────────────────┼────────────────┐
           │                │                │
           ▼                ▼                ▼
  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
  │   Worker     │  │   Worker     │  │   Worker     │
  │  Process 1   │  │  Process 2   │  │  Process 3   │
  │  (EC2 #1)    │  │  (EC2 #2)    │  │  (EC2 #3)    │
  │              │  │              │  │              │
  │  polls       │  │  polls       │  │  polls       │
  │  order-      │  │  order-      │  │  order-      │
  │  task-queue  │  │  task-queue  │  │  task-queue  │
  └──────────────┘  └──────────────┘  └──────────────┘

  All 3 poll the same queue.
  Temporal distributes tasks across them automatically.
  No coordination or shared state needed between workers.
  Kill any one — the others keep running, Temporal reschedules.
```

You just run more instances of the same binary. No load balancer config, no shared memory, no coordination logic — because all state lives in Temporal Server, not in the workers.

---

### What this looks like in production

```
AWS / GCP / Azure
│
├── Temporal Server
│   └── Temporal Cloud (managed) or self-hosted on ECS / K8s
│       Port 7233 (gRPC)   ← workers connect here
│       Port 8233 (Web UI) ← you monitor here
│
└── Your Worker Fleet  (auto-scaled ECS service or K8s Deployment)
    ├── ECS Task 1  ──  running worker binary  ┐
    ├── ECS Task 2  ──  running worker binary  ├── all poll order-task-queue
    ├── ECS Task 3  ──  running worker binary  ┘
    └── ECS Task N  ──  scales up/down based on queue backlog
```

Each ECS Task is one worker process. Workers are stateless — you can kill and replace any of them at any time without losing a single workflow execution.

---

## 7. Understanding Activities — 3 Functions, 3 Tasks

`activities.go` contains exactly 3 functions. Each function is one specific task that Temporal can schedule, execute, retry, and track independently.

```go
// Task 1 — validate the order data
func ValidateOrder(ctx context.Context, order Order) (string, error) { ... }

// Task 2 — charge the customer
func ChargePayment(ctx context.Context, order Order) (string, error) { ... }

// Task 3 — send confirmation email
func SendConfirmation(ctx context.Context, order Order) (string, error) { ... }
```

Yes — that's all they are. Plain Go functions. No special interface, no embedding, no annotations.

---

### Why separate functions instead of one big function?

This is the core design decision. Compare the two approaches:

**Without Temporal — one big function:**
```go
func processOrder(order Order) error {
    validate(order)   // step 1 — runs fine
    charge(order)     // step 2 — crashes here
    confirm(order)    // step 3 — never runs, no one knows
}
```
If it crashes at step 2, you have no idea what ran. You restart from scratch. Step 2 might charge the card twice.

**With Temporal — 3 separate activity functions:**
```go
// Each call is a separate checkpoint written to Temporal's history
workflow.ExecuteActivity(ctx, ValidateOrder, order)    // ✅ completed, recorded
workflow.ExecuteActivity(ctx, ChargePayment, order)    // 💥 worker crashes here
workflow.ExecuteActivity(ctx, SendConfirmation, order) // ← resumes exactly here
```
Temporal knows exactly which ones completed. On recovery it skips the done ones and picks up from `SendConfirmation`. The card is never charged twice.

---

### The 3 rules for an activity function

There are only 3 rules — everything else is up to you:

| Rule | Why |
|------|-----|
| First argument must be `context.Context` | Temporal uses it for cancellation and heartbeating |
| Last return value must be `error` | How Temporal knows if the task succeeded or failed |
| Should be idempotent where possible | Because Temporal may retry it — same input should produce same outcome |

That's it. They can do anything — DB queries, HTTP calls, file I/O, sending emails, calling external APIs.

---

### What these functions look like in a real application

In this project they simulate work with `fmt.Printf`. In production each would be a real integration:

```go
func ValidateOrder(ctx context.Context, order Order) (string, error) {
    // query your inventory database
    // call a fraud detection API
    // check customer credit limit
    // return error if any check fails → Temporal retries automatically
}

func ChargePayment(ctx context.Context, order Order) (string, error) {
    // call Stripe / PayPal API
    // write transaction record to your database
    // return error if payment fails → Temporal retries automatically
}

func SendConfirmation(ctx context.Context, order Order) (string, error) {
    // call SendGrid / AWS SES
    // write notification record to your database
    // return error if email fails → Temporal retries automatically
}
```

Each one is independently retryable. If `ChargePayment` fails because Stripe is temporarily down, Temporal retries just that function — `ValidateOrder` does not run again.

---

### How Temporal sees each function

When you register activities on the worker:

```go
w.RegisterActivity(orderworkflow.ValidateOrder)
w.RegisterActivity(orderworkflow.ChargePayment)
w.RegisterActivity(orderworkflow.SendConfirmation)
```

Temporal registers each function by its name as an **activity type**. When the workflow calls `workflow.ExecuteActivity(ctx, ValidateOrder, order)`, Temporal puts a task on the queue with type `"ValidateOrder"`. The worker picks it up, looks up the registered function by that name, and calls it. The result goes back to Temporal, which wakes the workflow up with it.

```
workflow.ExecuteActivity(ctx, ValidateOrder, order)
         │
         ▼
Temporal Server: schedule activity task type="ValidateOrder"
         │
         ▼
Worker: look up registered function "ValidateOrder" → call it → return result
         │
         ▼
Temporal Server: record ActivityTaskCompleted event
         │
         ▼
Workflow: resumes with the result string
```

---

## 8. Project Structure

```
temporal-order-workflow/
│
├── README.md               ← You are here
│
├── go.mod                  ← Go module definition, declares Temporal SDK dependency
│
├── workflow.go             ← Workflow definition + Order/OrderResult types
│                             Orchestrates the 3 steps in sequence
│
├── activities.go           ← The 3 activity functions (ValidateOrder, ChargePayment,
│                             SendConfirmation) — the actual work units
│
├── worker/
│   └── main.go             ← Long-running worker process
│                             Registers workflow + activities, polls task queue
│
└── starter/
    └── main.go             ← One-shot client
                              Submits the workflow execution, waits for result
```

---

## 9. Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Your Machine                                   │
│                                                                        │
│   ┌─────────────────┐              ┌──────────────────────────────┐   │
│   │    starter/     │              │         worker/              │   │
│   │    main.go      │              │         main.go              │   │
│   │                 │              │                              │   │
│   │  "Please run    │              │  RegisterWorkflow(...)       │   │
│   │  OrderWorkflow  │              │  RegisterActivity(...)       │   │
│   │  with this      │              │                              │   │
│   │  Order input"   │              │  Polls task queue in a loop  │   │
│   │                 │              │  Runs workflow.go            │   │
│   │  Waits for      │              │  Runs activities.go          │   │
│   │  result...      │              │                              │   │
│   └────────┬────────┘              └──────────────┬───────────────┘   │
│            │                                      │                   │
│            │  ExecuteWorkflow()                   │  Poll / Report    │
│            │                                      │                   │
└────────────┼──────────────────────────────────────┼───────────────────┘
             │                                      │
             ▼                                      ▲
┌──────────────────────────────────────────────────────────────────────┐
│                      Temporal Server                                  │
│                      localhost:7233                                   │
│                                                                        │
│   ┌──────────────────────────────────────────────────────────────┐   │
│   │  Task Queue: "order-task-queue"                               │   │
│   │                                                               │   │
│   │  Workflow Tasks  ──────────────────────────► Worker picks up  │   │
│   │  Activity Tasks  ──────────────────────────► Worker picks up  │   │
│   └──────────────────────────────────────────────────────────────┘   │
│                                                                        │
│   Stores full execution history durably                                │
│   Manages retries, timeouts, state                                     │
│                                                                        │
│   Web UI: localhost:8233                                               │
└──────────────────────────────────────────────────────────────────────┘
```

### Data flow in plain English

1. **Starter** connects to Temporal Server and says: "Start `OrderWorkflow` with this `Order` struct"
2. **Temporal Server** stores this request and puts a workflow task on `order-task-queue`
3. **Worker** is polling that queue, picks up the task, runs `OrderWorkflow`
4. **Workflow** calls `workflow.ExecuteActivity(ctx, ValidateOrder, order)` — this does NOT call the function directly. It tells Temporal to schedule an activity task
5. **Temporal Server** puts an activity task on the queue
6. **Worker** picks up the activity task, runs `ValidateOrder`, returns the result to Temporal
7. **Temporal** wakes the workflow back up with the result
8. Steps 4–7 repeat for `ChargePayment` and `SendConfirmation`
9. **Workflow** returns `OrderResult` — Temporal marks execution as Completed
10. **Starter** receives the result from `.Get()` and prints it

---

## 10. How the Code Works — File by File

### `workflow.go` — The Orchestrator

This file defines three things:

**The data types:**
```go
type Order struct {
    OrderID    string
    CustomerID string
    Item       string
    Amount     float64
}

type OrderResult struct {
    OrderID string
    Status  string   // "COMPLETED", "FAILED", "PARTIAL"
    Message string
}
```

**The task queue name** (shared constant used by both worker and starter):
```go
const TaskQueueName = "order-task-queue"
```

**The workflow function:**
```go
func OrderWorkflow(ctx workflow.Context, order Order) (OrderResult, error) {
```

Inside the workflow, activity options are set once and applied to all activities:
```go
activityOptions := workflow.ActivityOptions{
    StartToCloseTimeout: 10 * time.Second,  // each activity has 10s to finish
    RetryPolicy: &temporal.RetryPolicy{
        MaximumAttempts:    3,               // retry up to 3 times
        InitialInterval:    time.Second,     // wait 1s before first retry
        BackoffCoefficient: 2.0,             // 1s → 2s → 4s between retries
        MaximumInterval:    10 * time.Second,
    },
}
```

Then each activity is called in sequence. If any step fails after all retries, the workflow returns a FAILED result immediately — the remaining steps don't run:
```go
err := workflow.ExecuteActivity(ctx, ValidateOrder, order).Get(ctx, &validationResult)
if err != nil {
    return OrderResult{Status: "FAILED", ...}, err
}
// only reaches here if ValidateOrder succeeded
err = workflow.ExecuteActivity(ctx, ChargePayment, order).Get(ctx, &paymentResult)
// ...
```

---

### `activities.go` — The Workers

Three plain Go functions. Each takes `context.Context` as the first argument (required by Temporal) and the `Order` struct.

```go
func ValidateOrder(ctx context.Context, order Order) (string, error) {
    if order.OrderID == "" { return "", fmt.Errorf("order ID cannot be empty") }
    if order.Amount <= 0  { return "", fmt.Errorf("amount must be positive") }
    if order.Item == ""   { return "", fmt.Errorf("item cannot be empty") }
    // In real life: query DB, check inventory, fraud check...
    return "Order validated successfully", nil
}
```

```go
func ChargePayment(ctx context.Context, order Order) (string, error) {
    // In real life: call Stripe, PayPal, etc.
    transactionID := fmt.Sprintf("txn_%s_001", order.OrderID)
    return fmt.Sprintf("Payment of $%.2f charged. Transaction: %s", ...), nil
}
```

```go
func SendConfirmation(ctx context.Context, order Order) (string, error) {
    // In real life: call SendGrid, SES, etc.
    return fmt.Sprintf("Confirmation email sent to customer %s", ...), nil
}
```

These are intentionally simple. In a real application, each would call an external service. The key point is that if any of them return an error, Temporal retries them automatically — you don't write retry loops.

---

### `worker/main.go` — The Executor

```go
func main() {
    // 1. Connect to Temporal Server
    c, err := client.Dial(client.Options{HostPort: "localhost:7233"})

    // 2. Create a worker bound to our task queue
    w := worker.New(c, orderworkflow.TaskQueueName, worker.Options{})

    // 3. Tell the worker which functions it can execute
    w.RegisterWorkflow(orderworkflow.OrderWorkflow)
    w.RegisterActivity(orderworkflow.ValidateOrder)
    w.RegisterActivity(orderworkflow.ChargePayment)
    w.RegisterActivity(orderworkflow.SendConfirmation)

    // 4. Start polling — blocks until Ctrl+C
    w.Run(worker.InterruptCh())
}
```

The worker must register every workflow and activity it handles. If a task arrives for an unregistered function, the worker won't know how to handle it.

You can run multiple instances of this worker process — Temporal distributes tasks across all of them automatically.

---

### `starter/main.go` — The Trigger

```go
func main() {
    // 1. Connect to Temporal Server
    c, err := client.Dial(client.Options{HostPort: "localhost:7233"})

    // 2. Define the input
    order := orderworkflow.Order{
        OrderID:    "ORD-20260512-001",
        CustomerID: "CUST-42",
        Item:       "Mechanical Keyboard",
        Amount:     149.99,
    }

    // 3. Submit the workflow execution
    we, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
        ID:        "order-workflow-" + order.OrderID,  // unique ID per order
        TaskQueue: orderworkflow.TaskQueueName,
    }, orderworkflow.OrderWorkflow, order)

    // 4. Wait for the result
    var result orderworkflow.OrderResult
    we.Get(ctx, &result)

    fmt.Printf("Status: %s\nMessage: %s\n", result.Status, result.Message)
}
```

The `WorkflowID` is important — it's the unique identifier for this execution. If you try to start a workflow with the same ID while one is already running, Temporal rejects it. This gives you built-in idempotency.

---

## 11. The Full Execution Flow

Here is exactly what happens when you run `go run starter/main.go`, mapped to the 23 events Temporal recorded:

```
Starter runs
    │
    ├─► Event 1:  WorkflowExecutionStarted
    │             Temporal receives the request, stores it
    │
    ├─► Event 2:  WorkflowTaskScheduled
    ├─► Event 3:  WorkflowTaskStarted       ← Worker picks up, runs OrderWorkflow
    ├─► Event 4:  WorkflowTaskCompleted     ← Workflow calls ExecuteActivity(ValidateOrder)
    │
    ├─► Event 5:  ActivityTaskScheduled     ← Temporal schedules ValidateOrder
    ├─► Event 6:  ActivityTaskStarted       ← Worker picks up, runs ValidateOrder()
    ├─► Event 7:  ActivityTaskCompleted     ← "Order validated successfully"
    │
    ├─► Event 8:  WorkflowTaskScheduled
    ├─► Event 9:  WorkflowTaskStarted       ← Workflow resumes with validation result
    ├─► Event 10: WorkflowTaskCompleted     ← Workflow calls ExecuteActivity(ChargePayment)
    │
    ├─► Event 11: ActivityTaskScheduled     ← Temporal schedules ChargePayment
    ├─► Event 12: ActivityTaskStarted       ← Worker picks up, runs ChargePayment()
    ├─► Event 13: ActivityTaskCompleted     ← "Payment of $149.99 charged"
    │
    ├─► Event 14: WorkflowTaskScheduled
    ├─► Event 15: WorkflowTaskStarted       ← Workflow resumes with payment result
    ├─► Event 16: WorkflowTaskCompleted     ← Workflow calls ExecuteActivity(SendConfirmation)
    │
    ├─► Event 17: ActivityTaskScheduled     ← Temporal schedules SendConfirmation
    ├─► Event 18: ActivityTaskStarted       ← Worker picks up, runs SendConfirmation()
    ├─► Event 19: ActivityTaskCompleted     ← "Confirmation email sent"
    │
    ├─► Event 20: WorkflowTaskScheduled
    ├─► Event 21: WorkflowTaskStarted       ← Workflow resumes with confirmation result
    ├─► Event 22: WorkflowTaskCompleted     ← Workflow returns OrderResult{COMPLETED}
    │
    └─► Event 23: WorkflowExecutionCompleted
                  Total runtime: 80ms
```

The starter's `.Get()` call unblocks and prints the result.

---

## 12. Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Go | 1.21+ | Run the worker and starter |
| Temporal CLI | 1.7.0+ | Run the local dev server + monitoring commands |

### Install Go
```bash
winget install GoLang.Go
```
Or download from https://go.dev/dl/

### Install Temporal CLI
```bash
winget install Temporal.TemporalCLI
```
Or download from https://github.com/temporalio/cli/releases

---

## 13. Running the Project Step by Step

You need **3 terminals** open at the same time.

---

### Terminal 1 — Start the Temporal Dev Server

```bash
temporal server start-dev --ui-port 8233
```

What this does:
- Starts a local Temporal Server on port `7233` (gRPC)
- Starts the Web UI on port `8233`
- Uses an in-memory SQLite database (data is lost when you stop it — fine for dev)

Expected output:
```
Temporal CLI 1.7.0 (Server 1.31.0, UI 2.49.1)

Temporal Server:  localhost:7233
Persistence:      memory
Namespace:        default

Web UI:           http://localhost:8233
```

Leave this running.

---

### Terminal 2 — Start the Worker

```bash
cd temporal-order-workflow
go run worker/main.go
```

What this does:
- Connects to Temporal Server at `localhost:7233`
- Registers `OrderWorkflow`, `ValidateOrder`, `ChargePayment`, `SendConfirmation`
- Starts polling `order-task-queue` for tasks

Expected output:
```
2026/05/12 09:42:19 Worker started. Polling task queue: "order-task-queue"
2026/05/12 09:42:19 Waiting for workflow tasks... (press Ctrl+C to stop)
2026/05/12 09:42:19 INFO  Started Worker Namespace default TaskQueue order-task-queue WorkerID ...
```

Leave this running. The worker must be running before you submit a workflow.

---

### Terminal 3 — Submit the Workflow

```bash
cd temporal-order-workflow
go run starter/main.go
```

What this does:
- Connects to Temporal Server
- Submits `OrderWorkflow` with order `ORD-20260512-001`
- Waits for the workflow to complete
- Prints the result

Expected output:
```
2026/05/12 09:42:51 Submitting OrderWorkflow for order: ORD-20260512-001
2026/05/12 09:42:51 Workflow started — WorkflowID: order-workflow-ORD-20260512-001, RunID: 019e196c-...
2026/05/12 09:42:51 Waiting for workflow to complete...

── Workflow Result ──────────────────────────
  Order ID : ORD-20260512-001
  Status   : COMPLETED
  Message  : Order processed successfully. Confirmation email sent to customer CUST-42 for order ORD-20260512-001
─────────────────────────────────────────────
```

Meanwhile, in Terminal 2 (worker), you'll see each activity executing:
```
[Activity] ValidateOrder: order ORD-20260512-001 for item 'Mechanical Keyboard' is valid
[Activity] ChargePayment: charging $149.99 for order ORD-20260512-001 (customer: CUST-42)
[Activity] SendConfirmation: sending confirmation for order ORD-20260512-001 to customer CUST-42
```

---

## 14. Monitoring with CLI

After running the workflow, use the Temporal CLI to inspect it.

### List all workflow executions
```bash
temporal workflow list --namespace default
```
Output:
```
Status      WorkflowId                        Type           StartTime
Completed   order-workflow-ORD-20260512-001   OrderWorkflow  18 seconds ago
```

### Describe a specific execution (summary + result)
```bash
temporal workflow describe --workflow-id "order-workflow-ORD-20260512-001"
```
Output includes:
```
WorkflowId     order-workflow-ORD-20260512-001
RunId          019e196c-0913-710d-b94c-287a7acff9df
Type           OrderWorkflow
TaskQueue      order-task-queue
StartTime      28 seconds ago
CloseTime      28 seconds ago
RunTime        80ms
Status         COMPLETED
Result         {"Message":"Order processed successfully...","Status":"COMPLETED"}
```

### Show the full event history
```bash
temporal workflow show --workflow-id "order-workflow-ORD-20260512-001"
```
Output shows all 23 events — every workflow task, every activity scheduled/started/completed. This is the audit trail Temporal keeps for every execution.

### Check the task queue and worker health
```bash
temporal task-queue describe --task-queue "order-task-queue"
```
Output:
```
Task Queue Statistics:
  UNVERSIONED  workflow  BacklogCount: 0
  UNVERSIONED  activity  BacklogCount: 0

Pollers:
  UNVERSIONED  workflow  24528@DESKTOP-R05A0SB@  last seen: 52s ago
  UNVERSIONED  activity  24528@DESKTOP-R05A0SB@  last seen: 52s ago
```

`BacklogCount: 0` means no tasks are waiting — the worker is keeping up. The Pollers section shows which worker instances are connected.

---

## 15. Monitoring with the Web UI

Open **http://localhost:8233** in your browser.

### Workflows page
Shows all executions with status, workflow ID, type, and start time. You can filter by status (Running, Completed, Failed, etc.).

### Execution detail page
Click on any workflow execution to see:
- **Summary** — WorkflowID, RunID, status, duration, input, output
- **Event History** — every single event in the execution timeline, with timestamps and payloads
- **Pending Activities** — if the workflow is still running, shows which activity is currently executing

### What to look for
- Each activity shows as 3 events: `Scheduled → Started → Completed`
- The input and output of each activity is stored and visible
- If an activity failed and was retried, you'll see multiple `Started` events before a `Completed`

---

## 16. What the Logs Tell You

### Worker log breakdown

```
INFO  OrderWorkflow started ... orderID ORD-20260512-001
```
The workflow function started executing on the worker.

```
DEBUG ExecuteActivity ... ActivityType ValidateOrder
```
The workflow called `ExecuteActivity` — Temporal is scheduling the activity.

```
[Activity] ValidateOrder: order ORD-20260512-001 for item 'Mechanical Keyboard' is valid
```
This is the `fmt.Printf` inside `ValidateOrder` — the activity is actually running.

```
INFO  Order validated ... result Order ORD-20260512-001 validated successfully
```
The workflow received the activity result and logged it.

This pattern repeats for `ChargePayment` and `SendConfirmation`.

---

## 17. What Happens When Things Fail

### If an activity returns an error
Temporal retries it automatically. With our retry policy:
- Attempt 1 fails → wait 1 second → Attempt 2
- Attempt 2 fails → wait 2 seconds → Attempt 3
- Attempt 3 fails → workflow receives the error, returns `FAILED`

The workflow function never re-runs from the top. Only the failed activity is retried.

### If the worker crashes mid-workflow
Temporal detects the worker is gone (via heartbeat timeout). It reschedules the current task on the queue. When the worker restarts, it picks up where it left off — the completed activities are not re-run.

### If Temporal Server restarts
All workflow state is persisted to its database. When the server comes back up, all in-progress workflows resume automatically.

### If you submit the same WorkflowID twice
```
temporal: workflow execution already started
```
Temporal rejects the duplicate. This is built-in idempotency — safe to retry the starter without double-charging.

---

## Quick Reference

```bash
# Start dev server
temporal server start-dev --ui-port 8233

# Start worker
go run worker/main.go

# Submit workflow
go run starter/main.go

# List executions
temporal workflow list

# Inspect execution
temporal workflow describe --workflow-id "order-workflow-ORD-20260512-001"

# Full event history
temporal workflow show --workflow-id "order-workflow-ORD-20260512-001"

# Check worker health
temporal task-queue describe --task-queue "order-task-queue"

# Web UI
open http://localhost:8233
```
