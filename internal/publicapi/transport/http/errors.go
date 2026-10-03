package publichttp

import (
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func writeGRPCError(w http.ResponseWriter, err error) {
	grpcStatus := status.Convert(err)

	switch grpcStatus.Code() {
	case codes.InvalidArgument:
		writeError(w, http.StatusBadRequest, grpcStatus.Message())

	case codes.NotFound:
		writeError(w, http.StatusNotFound, grpcStatus.Message())

	case codes.Unavailable:
		writeError(w, http.StatusServiceUnavailable, "service unavailable")

	case codes.DeadlineExceeded:
		writeError(w, http.StatusGatewayTimeout, "upstream request timed out")

	case codes.FailedPrecondition:
		writeError(w, http.StatusConflict, grpcStatus.Message())

	default:
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}
