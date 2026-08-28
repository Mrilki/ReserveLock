package adminhttp

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func writeGRPCError(c *gin.Context, err error) {
	switch status.Code(err) {
	case codes.InvalidArgument:
		c.JSON(http.StatusBadRequest, gin.H{
			"error": status.Convert(err).Message(),
		})

	case codes.NotFound:
		c.JSON(http.StatusBadRequest, gin.H{
			"error": status.Convert(err).Message(),
		})
	case codes.FailedPrecondition:
		c.JSON(http.StatusBadRequest, gin.H{
			"error": status.Convert(err).Message(),
		})

	case codes.Unavailable:
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "service unavailable",
		})
	case codes.DeadlineExceeded:
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "upstream request timed out",
		})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "internal error",
		})
	}

}
