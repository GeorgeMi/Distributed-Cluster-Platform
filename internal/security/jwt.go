package security

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// TokenPair contains an access token and a refresh token.
type TokenPair struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
	ExpiresIn    int64  `json:"expiresIn"`
}

// Claims holds the JWT payload.
type Claims struct {
	UserID    string `json:"userID"`
	Role      string `json:"role"`
	TokenType string `json:"type"`
	jwt.RegisteredClaims
}

// RefreshClaims holds the refresh token payload.
type RefreshClaims struct {
	UserID    string `json:"userID"`
	TokenType string `json:"type"`
	jwt.RegisteredClaims
}

// JWTManager handles token generation and validation.
type JWTManager struct {
	secret          []byte
	accessDuration  time.Duration
	refreshDuration time.Duration
}

func NewJWTManager(secret string, accessDuration, refreshDuration time.Duration) *JWTManager {
	return &JWTManager{
		secret:          []byte(secret),
		accessDuration:  accessDuration,
		refreshDuration: refreshDuration,
	}
}

// GenerateTokenPair creates an access token and a refresh token.
func (m *JWTManager) GenerateTokenPair(userID, role string) (*TokenPair, error) {
	// Access token
	now := time.Now()
	accessClaims := Claims{
		UserID:    userID,
		Role:      role,
		TokenType: "access",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(m.accessDuration)),
			IssuedAt:  jwt.NewNumericDate(now),
		},
	}
	accessToken := jwt.NewWithClaims(jwt.SigningMethodHS256, accessClaims)
	accessStr, err := accessToken.SignedString(m.secret)
	if err != nil {
		return nil, fmt.Errorf("sign access token: %w", err)
	}

	// Refresh token
	refreshClaims := RefreshClaims{
		UserID:    userID,
		TokenType: "refresh",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(m.refreshDuration)),
			IssuedAt:  jwt.NewNumericDate(now),
		},
	}
	refreshToken := jwt.NewWithClaims(jwt.SigningMethodHS256, refreshClaims)
	refreshStr, err := refreshToken.SignedString(m.secret)
	if err != nil {
		return nil, fmt.Errorf("sign refresh token: %w", err)
	}

	return &TokenPair{
		AccessToken:  accessStr,
		RefreshToken: refreshStr,
		ExpiresIn:    int64(m.accessDuration.Seconds()),
	}, nil
}

// ValidateAccessToken parses and validates an access token, returns the claims.
func (m *JWTManager) ValidateAccessToken(tokenStr string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &Claims{}, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return m.secret, nil
	})
	if err != nil {
		return nil, fmt.Errorf("invalid token: %w", err)
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid || claims.TokenType != "access" {
		return nil, fmt.Errorf("invalid token claims")
	}
	return claims, nil
}

// ValidateRefreshToken parses and validates a refresh token, returns the user ID.
func (m *JWTManager) ValidateRefreshToken(tokenStr string) (string, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &RefreshClaims{}, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return m.secret, nil
	})
	if err != nil {
		return "", fmt.Errorf("invalid refresh token: %w", err)
	}

	claims, ok := token.Claims.(*RefreshClaims)
	if !ok || !token.Valid || claims.TokenType != "refresh" {
		return "", fmt.Errorf("invalid refresh token claims")
	}
	return claims.UserID, nil
}
