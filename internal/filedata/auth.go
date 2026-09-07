package filedata

import (
	"context"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func appendBearerToken(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func bearerUnaryServerInterceptor(token string) grpc.UnaryServerInterceptor {
	expected := "Bearer " + strings.TrimSpace(token)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !authorizedContext(ctx, expected) {
			return nil, status.Error(codes.Unauthenticated, "filedata grpc: invalid bearer token")
		}
		return handler(ctx, req)
	}
}

func bearerStreamServerInterceptor(token string) grpc.StreamServerInterceptor {
	expected := "Bearer " + strings.TrimSpace(token)
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !authorizedContext(stream.Context(), expected) {
			return status.Error(codes.Unauthenticated, "filedata grpc: invalid bearer token")
		}
		return handler(srv, stream)
	}
}

func authorizedContext(ctx context.Context, expected string) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	values := md.Get("authorization")
	for _, value := range values {
		if subtle.ConstantTimeCompare([]byte(value), []byte(expected)) == 1 {
			return true
		}
	}
	return false
}
