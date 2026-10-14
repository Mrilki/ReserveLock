package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestCheckHTTP(t *testing.T) {
	for _, code := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(code)
			}))
			defer server.Close()

			ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
			defer cancel()

			err := checkHTTP(ctx, server.URL)
			if (err == nil) != (code == http.StatusOK) {
				t.Fatalf("status=%d err=%v", code, err)
			}
		})
	}
}

func TestCheckGRPC(t *testing.T) {
	for _, servingStatus := range []healthpb.HealthCheckResponse_ServingStatus{
		healthpb.HealthCheckResponse_SERVING,
		healthpb.HealthCheckResponse_NOT_SERVING,
	} {
		t.Run(servingStatus.String(), func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = listener.Close()
			}()

			server := grpc.NewServer()
			healthServer := health.NewServer()
			healthServer.SetServingStatus("", servingStatus)
			healthpb.RegisterHealthServer(server, healthServer)
			go func() {
				_ = server.Serve(listener)
			}()
			defer server.Stop()

			ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
			defer cancel()

			err = checkGRPC(ctx, listener.Addr().String())
			if (err == nil) != (servingStatus == healthpb.HealthCheckResponse_SERVING) {
				t.Fatalf("status=%s err=%v", servingStatus, err)
			}
		})
	}
}
