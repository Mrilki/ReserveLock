package adminhttp

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func writeGRPCError(c *gin.Context, err error) {
	grpcStatus := status.Convert(err)

	switch grpcStatus.Code() {
	case codes.InvalidArgument:
		c.JSON(http.StatusBadRequest, gin.H{
			"error": grpcStatus.Message(),
		})

	case codes.NotFound:
		c.JSON(http.StatusNotFound, gin.H{
			"error": grpcStatus.Message(),
		})

	case codes.FailedPrecondition:
		c.JSON(http.StatusConflict, gin.H{
			"error": grpcStatus.Message(),
		})

	case codes.Unavailable:
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "service unavailable",
		})

	case codes.DeadlineExceeded:
		c.JSON(http.StatusGatewayTimeout, gin.H{
			"error": "upstream request timed out",
		})

	default:
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "internal error",
		})
	}
}
