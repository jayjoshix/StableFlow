// Package auth provides JWT validation and API-key authentication
// middleware for StableFlow services. For production, integrate with
// a real identity provider (Auth0, Clerk, etc.) and replace the
// HMAC-SHA256 signer with RS256/ES256 backed by a JWKS endpoint.
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// ── Configuration ─────────────────────────────────────────────

// Config holds authentication settings.
type Config struct {
	// JWTSecret is the HMAC-SHA256 signing key (base64-encoded for production).
	JWTSecret string
	// APIKeys is a set of valid static API keys (for service-to-service auth).
	APIKeys []string
	// TokenExpiry is the lifetime of issued tokens.
	TokenExpiry time.Duration
}

// ── Claims ────────────────────────────────────────────────────

// Claims represents the JWT payload for StableFlow tokens.
type Claims struct {
	Sub   string   `json:"sub"`            // subject (user or service ID)
	Roles []string `json:"roles"`          // e.g. ["admin", "payments.write"]
	Iss   string   `json:"iss"`            // issuer
	Aud   string   `json:"aud,omitempty"`  // audience
	Exp   int64    `json:"exp"`            // expiry (unix timestamp)
	Iat   int64    `json:"iat"`            // issued at
	Jti   string   `json:"jti,omitempty"`  // unique token ID
}

// IsExpired returns true if the token has expired.
func (c Claims) IsExpired() bool {
	return time.Now().Unix() > c.Exp
}

// HasRole checks whether the claims include the given role.
func (c Claims) HasRole(role string) bool {
	for _, r := range c.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// ── Token creation (HMAC-SHA256) ──────────────────────────────

// IssueToken creates a signed JWT string using HMAC-SHA256.
// TODO: Replace with RS256/ES256 for production.
func IssueToken(secret string, claims Claims) (string, error) {
	header := base64URLEncode([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("auth: failed to marshal claims: %w", err)
	}
	payloadEnc := base64URLEncode(payload)
	signingInput := header + "." + payloadEnc
	sig := sign([]byte(secret), []byte(signingInput))
	return signingInput + "." + base64URLEncode(sig), nil
}

// ── Token validation ──────────────────────────────────────────

// ValidateToken parses and validates a JWT string, returning the claims.
func ValidateToken(secret, tokenStr string) (*Claims, error) {
	parts := strings.SplitN(tokenStr, ".", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("auth: malformed token")
	}
	signingInput := parts[0] + "." + parts[1]
	expectedSig := sign([]byte(secret), []byte(signingInput))
	actualSig, err := base64URLDecode(parts[2])
	if err != nil {
		return nil, fmt.Errorf("auth: invalid signature encoding: %w", err)
	}
	if !hmac.Equal(expectedSig, actualSig) {
		return nil, fmt.Errorf("auth: invalid signature")
	}
	payload, err := base64URLDecode(parts[1])
	if err != nil {
		return nil, fmt.Errorf("auth: invalid payload encoding: %w", err)
	}
	var claims Claims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, fmt.Errorf("auth: invalid claims: %w", err)
	}
	if claims.IsExpired() {
		return nil, fmt.Errorf("auth: token expired")
	}
	return &claims, nil
}

// ── API Key validation ────────────────────────────────────────

// ValidateAPIKey returns true if the key matches any configured key
// using constant-time comparison.
func ValidateAPIKey(validKeys []string, key string) bool {
	for _, k := range validKeys {
		if subtle.ConstantTimeCompare([]byte(k), []byte(key)) == 1 {
			return true
		}
	}
	return false
}

// ── HTTP Middleware ────────────────────────────────────────────

type claimsKey struct{}

// Middleware returns an HTTP middleware that validates either a Bearer
// JWT or an X-API-Key header. On success, the Claims are injected
// into the request context.
func Middleware(cfg Config, logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip health check endpoints.
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			next.ServeHTTP(w, r)
			return
		}

		// Try Bearer token first.
		if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
			token := strings.TrimPrefix(auth, "Bearer ")
			claims, err := ValidateToken(cfg.JWTSecret, token)
			if err != nil {
				logger.WarnContext(r.Context(), "jwt validation failed", "error", err)
				writeAuthError(w, "invalid or expired token")
				return
			}
			ctx := context.WithValue(r.Context(), claimsKey{}, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		// Fall back to API key.
		if key := r.Header.Get("X-API-Key"); key != "" {
			if ValidateAPIKey(cfg.APIKeys, key) {
				// Synthetic claims for API-key auth.
				claims := &Claims{
					Sub:   "apikey",
					Roles: []string{"service"},
					Iss:   "stableflow",
					Exp:   time.Now().Add(time.Hour).Unix(),
					Iat:   time.Now().Unix(),
				}
				ctx := context.WithValue(r.Context(), claimsKey{}, claims)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			logger.WarnContext(r.Context(), "invalid API key")
			writeAuthError(w, "invalid API key")
			return
		}

		writeAuthError(w, "missing authentication")
	})
}

// ClaimsFromContext retrieves the validated claims from the request context.
func ClaimsFromContext(ctx context.Context) *Claims {
	if c, ok := ctx.Value(claimsKey{}).(*Claims); ok {
		return c
	}
	return nil
}

// RequireRole returns middleware that enforces the caller has a specific role.
func RequireRole(role string, logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims := ClaimsFromContext(r.Context())
		if claims == nil || !claims.HasRole(role) {
			logger.WarnContext(r.Context(), "insufficient permissions", "required", role)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprintf(w, `{"error":"forbidden","required_role":%q}`, role)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ── Helpers ───────────────────────────────────────────────────

func sign(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func base64URLEncode(data []byte) string {
	return base64.RawURLEncoding.EncodeToString(data)
}

func base64URLDecode(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}

func writeAuthError(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	fmt.Fprintf(w, `{"error":%q}`, msg)
}
