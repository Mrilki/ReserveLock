package analyticsgrpc

import (
	"context"

	"github.com/Mrilki/ReserveLock/internal/analytics/application"
	analyticsv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/analytics/v1"
	"go.uber.org/zap"
)

type server struct {
	analyticsv1.UnimplementedAnalyticsServiceServer
	service *application.Service
	logger  *zap.Logger
}

func NewServer(service *application.Service, logger *zap.Logger) analyticsv1.AnalyticsServiceServer {
	return &server{
		service: service,
		logger:  logger,
	}
}

func (s *server) GetEventSummary(
	ctx context.Context,
	request *analyticsv1.GetEventSummaryRequest,
) (*analyticsv1.GetEventSummaryResponse, error) {
	summary, err := s.service.GetEventSummary(ctx, request.GetEventId())
	if err != nil {
		return nil, s.rpcError("GetEventSummary", err)
	}

	return &analyticsv1.GetEventSummaryResponse{
		Summary: toProtoEventSummary(summary),
	}, nil
}
