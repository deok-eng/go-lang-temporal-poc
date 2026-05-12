package orderworkflow

import (
	"context"
	"fmt"
)

// Activities are plain Go functions — no special interface needed.
//
// Rules for activities:
//   - First argument must be context.Context
//   - They CAN have side effects (DB writes, HTTP calls, etc.)
//   - They are NOT required to be deterministic (unlike workflows)
//   - They should be idempotent when possible, since Temporal may retry them
//
// In a real app these would call databases, payment APIs, email services, etc.
// Here we simulate the work with simple print statements.

// ValidateOrder checks that the order data is valid and stock is available.
func ValidateOrder(ctx context.Context, order Order) (string, error) {
	// Simulate validation logic
	if order.OrderID == "" {
		return "", fmt.Errorf("order ID cannot be empty")
	}
	if order.Amount <= 0 {
		return "", fmt.Errorf("order amount must be positive, got %.2f", order.Amount)
	}
	if order.Item == "" {
		return "", fmt.Errorf("order must contain at least one item")
	}

	// In a real app: query inventory DB, validate customer ID, check fraud rules, etc.
	fmt.Printf("[Activity] ValidateOrder: order %s for item '%s' is valid\n",
		order.OrderID, order.Item)

	return fmt.Sprintf("Order %s validated successfully", order.OrderID), nil
}

// ChargePayment processes the payment for the order.
func ChargePayment(ctx context.Context, order Order) (string, error) {
	// In a real app: call Stripe/PayPal/etc., record transaction in DB
	fmt.Printf("[Activity] ChargePayment: charging $%.2f for order %s (customer: %s)\n",
		order.Amount, order.OrderID, order.CustomerID)

	// Simulate a successful payment
	transactionID := fmt.Sprintf("txn_%s_001", order.OrderID)

	return fmt.Sprintf("Payment of $%.2f charged successfully. Transaction: %s",
		order.Amount, transactionID), nil
}

// SendConfirmation sends an order confirmation to the customer.
func SendConfirmation(ctx context.Context, order Order) (string, error) {
	// In a real app: send email via SendGrid/SES, push notification, SMS, etc.
	fmt.Printf("[Activity] SendConfirmation: sending confirmation for order %s to customer %s\n",
		order.OrderID, order.CustomerID)

	return fmt.Sprintf("Confirmation email sent to customer %s for order %s",
		order.CustomerID, order.OrderID), nil
}
