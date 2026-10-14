package adminhttp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	analyticsv1 "github.com/Mrilki/ReserveLock/internal/gen/proto/reservelock/analytics/v1"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testAnalyticsClient struct {
	response *analyticsv1.GetEventSummaryResponse
	err      error
	eventID  string
}

func (c *testAnalyticsClient) GetEventSummary(_ context.Context, request *analyticsv1.GetEventSummaryRequest, _ ...grpc.CallOption) (*analyticsv1.GetEventSummaryResponse, error) {
	c.eventID = request.GetEventId()
	return c.response, c.err
}

func TestGetEventSummaryHTTP(t *testing.T) {
	tests := []struct {
		name          string
		response      *analyticsv1.GetEventSummaryResponse
		err           error
		expectedCode  int
		expectedBody  EventSummaryResponse
		expectedError string
	}{
		{
			name: "summary",
			response: &analyticsv1.GetEventSummaryResponse{
				Summary: &analyticsv1.EventSummary{
					EventId: "event-1", Created: 5, Confirmed: 2, Expired: 1,
					CreatedSeats: 10, ConfirmedSeats: 4, ExpiredSeats: 2,
					ConfirmationRatePercent: 40, ExpirationRatePercent: 20,
					AvgConfirmationSeconds: 30.5, AvgExpirationLagSeconds: 2.25,
					Daily: []*analyticsv1.EventDailySummary{
						{
							Date: "2026-10-14", Created: 5, Confirmed: 2, Expired: 1,
							CreatedSeats: 10, ConfirmedSeats: 4, ExpiredSeats: 2,
						},
					},
				},
			},
			expectedCode: http.StatusOK,
			expectedBody: EventSummaryResponse{
				EventID: "event-1", Created: 5, Confirmed: 2, Expired: 1,
				CreatedSeats: 10, ConfirmedSeats: 4, ExpiredSeats: 2,
				ConfirmationRatePercent: 40, ExpirationRatePercent: 20,
				AvgConfirmationSeconds: 30.5, AvgExpirationLagSeconds: 2.25,
				Daily: []EventDailySummaryResponse{
					{
						Date: "2026-10-14", Created: 5, Confirmed: 2, Expired: 1,
						CreatedSeats: 10, ConfirmedSeats: 4, ExpiredSeats: 2,
					},
				},
			},
		},
		{
			name: "no facts",
			response: &analyticsv1.GetEventSummaryResponse{
				Summary: &analyticsv1.EventSummary{EventId: "event-1"},
			},
			expectedCode: http.StatusOK,
			expectedBody: EventSummaryResponse{
				EventID: "event-1",
				Daily:   []EventDailySummaryResponse{},
			},
		},
		{
			name:          "unavailable",
			err:           status.Error(codes.Unavailable, "connection details"),
			expectedCode:  http.StatusServiceUnavailable,
			expectedError: "service unavailable",
		},
		{
			name:          "deadline",
			err:           status.Error(codes.DeadlineExceeded, "deadline"),
			expectedCode:  http.StatusGatewayTimeout,
			expectedError: "upstream request timed out",
		},
		{
			name:          "missing summary",
			response:      &analyticsv1.GetEventSummaryResponse{},
			expectedCode:  http.StatusInternalServerError,
			expectedError: "internal error",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &testAnalyticsClient{response: test.response, err: test.err}
			handler := NewHandler(nil, client)

			router := gin.New()
			router.GET("/admin/events/:id/analytics", handler.GetEventSummary)

			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/admin/events/event-1/analytics", nil)
			router.ServeHTTP(recorder, request)

			if recorder.Code != test.expectedCode {
				t.Fatalf("status: got %d, want %d; body=%s", recorder.Code, test.expectedCode, recorder.Body)
			}
			if client.eventID != "event-1" {
				t.Fatalf("unexpected upstream event id: %q", client.eventID)
			}

			if test.expectedError != "" {
				var body struct {
					Error string `json:"error"`
				}
				if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
					t.Fatal(err)
				}
				if body.Error != test.expectedError {
					t.Fatalf("error: got %q, want %q", body.Error, test.expectedError)
				}
				return
			}

			var body EventSummaryResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(body, test.expectedBody) {
				t.Fatalf("body: got %+v, want %+v", body, test.expectedBody)
			}

			var fields map[string]json.RawMessage
			if err := json.Unmarshal(recorder.Body.Bytes(), &fields); err != nil {
				t.Fatal(err)
			}
			if len(fields) != 12 {
				t.Fatalf("expected all twelve response fields, got %s", recorder.Body)
			}
		})
	}
}
