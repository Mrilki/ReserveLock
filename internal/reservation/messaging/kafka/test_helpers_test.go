package kafka

import (
	"context"
	"testing"

	"github.com/Mrilki/ReserveLock/internal/reservation/application"
)

type serviceStub struct {
	t *testing.T

	eventPublished   func(context.Context, application.EventPublishedInput) (bool, error)
	paymentSucceeded func(context.Context, application.PaymentResultInput) (bool, error)
	paymentFailed    func(context.Context, application.PaymentResultInput) (bool, error)
}

func (s *serviceStub) HandleEventPublished(
	ctx context.Context,
	input application.EventPublishedInput,
) (bool, error) {
	s.t.Helper()

	if s.eventPublished == nil {
		s.t.Fatal("unexpected HandleEventPublished call")
	}
	return s.eventPublished(ctx, input)
}

func (s *serviceStub) HandlePaymentSucceeded(
	ctx context.Context,
	input application.PaymentResultInput,
) (bool, error) {
	s.t.Helper()

	if s.paymentSucceeded == nil {
		s.t.Fatal("unexpected HandlePaymentSucceeded call")
	}
	return s.paymentSucceeded(ctx, input)
}

func (s *serviceStub) HandlePaymentFailed(
	ctx context.Context,
	input application.PaymentResultInput,
) (bool, error) {
	s.t.Helper()

	if s.paymentFailed == nil {
		s.t.Fatal("unexpected HandlePaymentFailed call")
	}
	return s.paymentFailed(ctx, input)
}
