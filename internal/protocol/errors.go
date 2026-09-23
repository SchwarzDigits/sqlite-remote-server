package protocol

import (
	"errors"
	"fmt"
	"log/slog"

	pb "github.com/SchwarzDigits/sqlite-remote-server/internal/gen/sqlite_remote/v1"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/store"
)

var (
	errUnauthenticated = errors.New("unauthenticated")
	errTooLarge        = errors.New("commit exceeds the size limit")
)

func unauthenticated(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errUnauthenticated, fmt.Sprintf(format, args...))
}

// errorFrame returns the Error frame for err as the answer to requestID. Errors of unknown type are logged and sent
// to the client as ERROR_CODE_INTERNAL without details.
func errorFrame(requestID uint64, err error, log *slog.Logger) *pb.ServerFrame {
	e := &pb.Error{Detail: err.Error()}
	var (
		held     *store.LeaseHeldError
		conflict *store.VersionConflictError
		bad      *store.BadRequestError
	)
	switch {
	case errors.Is(err, errUnauthenticated):
		e.Code = pb.ErrorCode_ERROR_CODE_UNAUTHENTICATED
	case errors.Is(err, store.ErrNotFound):
		e.Code = pb.ErrorCode_ERROR_CODE_NOT_FOUND
	case errors.Is(err, store.ErrFenced):
		e.Code = pb.ErrorCode_ERROR_CODE_FENCED
	case errors.Is(err, errTooLarge):
		e.Code = pb.ErrorCode_ERROR_CODE_TOO_LARGE
	case errors.As(err, &held):
		e.Code = pb.ErrorCode_ERROR_CODE_LEASE_HELD
		e.LeaseHolderSinceMs = uint64(held.Since.UnixMilli())
	case errors.As(err, &conflict):
		e.Code = pb.ErrorCode_ERROR_CODE_VERSION_CONFLICT
		e.CurrentVersion = conflict.Current
	case errors.As(err, &bad):
		e.Code = pb.ErrorCode_ERROR_CODE_BAD_REQUEST
	default:
		log.Error("request failed", "request_id", requestID, "error", err)
		e.Code = pb.ErrorCode_ERROR_CODE_INTERNAL
		e.Detail = "internal error"
	}
	return &pb.ServerFrame{RequestId: requestID, Body: &pb.ServerFrame_Error{Error: e}}
}
