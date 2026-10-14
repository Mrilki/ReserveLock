package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const probeTimeout = 2 * time.Second

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: healthcheck <http|grpc> <address>")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

	var err error
	switch os.Args[1] {
	case "http":
		err = checkHTTP(ctx, os.Args[2])
	case "grpc":
		err = checkGRPC(ctx, os.Args[2])
	default:
		err = fmt.Errorf("unsupported health protocol %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func checkHTTP(ctx context.Context, address string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return fmt.Errorf("create http health request: %w", err)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("http health request: %w", err)
	}
	defer func() {
		_ = response.Body.Close()
	}()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("http health status: %d", response.StatusCode)
	}
	return nil
}

func checkGRPC(ctx context.Context, address string) error {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("create grpc health client: %w", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	response, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("grpc health request: %w", err)
	}
	if response.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("grpc health status: %s", response.GetStatus())
	}
	return nil
}
