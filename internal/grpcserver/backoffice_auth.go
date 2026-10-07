package grpcserver

import (
	"context"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const refundBackofficePrefix = "/bridgepay.refund.backoffice.v1.BackofficeRefundService/"
const refundGatewayPrefix = "/bridgepay.refund.v1.RefundGatewayService/"

type BackofficeUser struct {
	ID    string
	Name  string
	Email string
	Role  string
}

type backofficeUserContextKey struct{}
type backofficeRequestIDContextKey struct{}

func BackofficeUserFromContext(ctx context.Context) (BackofficeUser, bool) {
	user, ok := ctx.Value(backofficeUserContextKey{}).(BackofficeUser)
	return user, ok
}

func BackofficeRequestIDFromContext(ctx context.Context) string {
	requestID, _ := ctx.Value(backofficeRequestIDContextKey{}).(string)
	return requestID
}

func RequireBackofficeRoles(ctx context.Context, roles ...string) error {
	user, ok := BackofficeUserFromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "user authentication is required")
	}
	for _, role := range roles {
		if user.Role == role {
			return nil
		}
	}
	return status.Error(codes.PermissionDenied, "user is not authorized for this operation")
}

func clientIdentityInterceptor(jwtSecret, gatewayClientName, backofficeClientName string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		switch {
		case strings.HasPrefix(info.FullMethod, refundGatewayPrefix):
			if gatewayClientName != "" && !hasTLSClientIdentity(ctx, gatewayClientName) {
				return nil, status.Error(codes.Unauthenticated, "gateway service identity is required")
			}
		case strings.HasPrefix(info.FullMethod, refundBackofficePrefix):
			if backofficeClientName == "" || !hasTLSClientIdentity(ctx, backofficeClientName) {
				return nil, status.Error(codes.Unauthenticated, "backoffice service identity is required")
			}
			if jwtSecret == "" {
				return nil, status.Error(codes.FailedPrecondition, "user token verification is not configured")
			}
			user, err := parseBackofficeJWT(ctx, jwtSecret)
			if err != nil {
				return nil, err
			}
			ctx = context.WithValue(ctx, backofficeUserContextKey{}, user)
			if incoming, ok := metadata.FromIncomingContext(ctx); ok {
				if requestIDs := incoming.Get("x-request-id"); len(requestIDs) > 0 {
					ctx = context.WithValue(ctx, backofficeRequestIDContextKey{}, requestIDs[0])
				}
			}
		}
		return handler(ctx, request)
	}
}

func parseBackofficeJWT(ctx context.Context, secret string) (BackofficeUser, error) {
	incoming, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return BackofficeUser{}, status.Error(codes.Unauthenticated, "bearer token is required")
	}
	values := incoming.Get("authorization")
	if len(values) != 1 {
		return BackofficeUser{}, status.Error(codes.Unauthenticated, "bearer token is required")
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return BackofficeUser{}, status.Error(codes.Unauthenticated, "bearer token is invalid")
	}
	token, err := jwt.Parse(parts[1], func(token *jwt.Token) (any, error) {
		if token.Method != jwt.SigningMethodHS256 {
			return nil, status.Error(codes.Unauthenticated, "bearer token is invalid")
		}
		return []byte(secret), nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}))
	if err != nil || token == nil || !token.Valid {
		return BackofficeUser{}, status.Error(codes.Unauthenticated, "bearer token is invalid or expired")
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return BackofficeUser{}, status.Error(codes.Unauthenticated, "bearer token is invalid")
	}
	expiresAt, err := claims.GetExpirationTime()
	if err != nil || expiresAt == nil {
		return BackofficeUser{}, status.Error(codes.Unauthenticated, "bearer token is missing an expiry")
	}
	user := BackofficeUser{
		ID:    jwtStringClaim(claims, "id"),
		Name:  jwtStringClaim(claims, "name"),
		Email: jwtStringClaim(claims, "email"),
		Role:  jwtStringClaim(claims, "role"),
	}
	if user.ID == "" || user.Role == "" {
		return BackofficeUser{}, status.Error(codes.Unauthenticated, "bearer token is missing required claims")
	}
	return user, nil
}

func jwtStringClaim(claims jwt.MapClaims, key string) string {
	value, _ := claims[key].(string)
	return value
}

func hasTLSClientIdentity(ctx context.Context, expectedName string) bool {
	remote, ok := peer.FromContext(ctx)
	if !ok {
		return false
	}
	tlsInfo, ok := remote.AuthInfo.(credentials.TLSInfo)
	if !ok {
		if pointerInfo, pointerOK := remote.AuthInfo.(*credentials.TLSInfo); pointerOK {
			tlsInfo, ok = *pointerInfo, true
		}
	}
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
		return false
	}
	return tlsInfo.State.PeerCertificates[0].VerifyHostname(expectedName) == nil
}
