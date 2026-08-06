// Package vaultclient implements Vault's Kubernetes auth method login
// flow and Transit sign/verify calls — the mechanism registry/ uses to
// sign manifests at RegisterModelManifest (Step E) and verify them at
// GetModelManifest, and that Step J's IssueProviderToken will reuse for
// its own Vault access.
//
// Kubernetes auth, not a static Vault token in a Secret: control-plane
// authenticates to Vault using its own pod's projected ServiceAccount
// token (the cluster's own identity for it), which Vault validates via
// the Kubernetes TokenReview API against the bound role configured in
// scripts/vault-setup-control-plane.sh. This is the user's own explicit
// requirement — reusing a static credential to reach Vault would just
// move the exact problem Vault is replacing (a stored secret) one layer
// down.
package vaultclient

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
)

const defaultSATokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"

// Signer is the interface registry/ depends on — Client below is the real
// Vault-Transit-backed implementation; tests use a fake.
type Signer interface {
	Sign(ctx context.Context, keyName string, input []byte) (signature string, err error)
	Verify(ctx context.Context, keyName string, input []byte, signature string) (valid bool, err error)
}

// Client logs in via Vault's Kubernetes auth method on first use and
// re-logs-in once the cached token is within its own renewal buffer of
// expiring — off the hot path (Phase-04.txt's own framing), so a simple
// lazy-refresh-before-call is proportionate; no background renewal loop.
type Client struct {
	addr         string
	k8sRole      string
	saTokenPath  string
	httpClient   *http.Client
	mu           sync.Mutex
	vaultToken   string
	tokenExpires time.Time
}

func New(addr, k8sRole string) *Client {
	return &Client{
		addr:        addr,
		k8sRole:     k8sRole,
		saTokenPath: defaultSATokenPath,
		httpClient:  &http.Client{Timeout: 10 * time.Second},
	}
}

type k8sLoginRequest struct {
	Role string `json:"role"`
	JWT  string `json:"jwt"`
}

type vaultAuthResponse struct {
	Auth struct {
		ClientToken   string `json:"client_token"`
		LeaseDuration int    `json:"lease_duration"`
	} `json:"auth"`
	Errors []string `json:"errors"`
}

// login performs the Kubernetes auth method's login flow: read this pod's
// own projected ServiceAccount token (the cluster-vouched identity, not a
// stored credential) and exchange it for a Vault token scoped to
// k8sRole's policy.
func (c *Client) login(ctx context.Context) error {
	jwt, err := os.ReadFile(c.saTokenPath)
	if err != nil {
		return fmt.Errorf("reading service account token: %w", err)
	}

	body, err := json.Marshal(k8sLoginRequest{Role: c.k8sRole, JWT: string(jwt)})
	if err != nil {
		return fmt.Errorf("marshaling login request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.addr+"/v1/auth/kubernetes/login", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("building login request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("calling Vault kubernetes login: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading login response: %w", err)
	}

	var authResp vaultAuthResponse
	if err := json.Unmarshal(respBody, &authResp); err != nil {
		return fmt.Errorf("decoding login response (status %d): %w", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK || authResp.Auth.ClientToken == "" {
		return fmt.Errorf("vault kubernetes login failed (status %d): %v", resp.StatusCode, authResp.Errors)
	}

	c.vaultToken = authResp.Auth.ClientToken
	// 30s renewal buffer: re-login proactively rather than have a signing
	// call fail on an edge-of-expiry token.
	c.tokenExpires = time.Now().Add(time.Duration(authResp.Auth.LeaseDuration)*time.Second - 30*time.Second)
	return nil
}

func (c *Client) ensureToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.vaultToken != "" && time.Now().Before(c.tokenExpires) {
		return c.vaultToken, nil
	}
	if err := c.login(ctx); err != nil {
		return "", err
	}
	return c.vaultToken, nil
}

type transitSignRequest struct {
	Input string `json:"input"`
}

type transitSignResponse struct {
	Data struct {
		Signature string `json:"signature"`
	} `json:"data"`
	Errors []string `json:"errors"`
}

// Sign returns a Transit signature over input, in Vault's own
// "vault:v<version>:<base64>" format — opaque to callers, passed back to
// Verify unmodified.
func (c *Client) Sign(ctx context.Context, keyName string, input []byte) (string, error) {
	token, err := c.ensureToken(ctx)
	if err != nil {
		return "", fmt.Errorf("vault auth: %w", err)
	}

	reqBody, err := json.Marshal(transitSignRequest{Input: base64.StdEncoding.EncodeToString(input)})
	if err != nil {
		return "", fmt.Errorf("marshaling sign request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.addr+"/v1/transit/sign/"+keyName, bytes.NewReader(reqBody))
	if err != nil {
		return "", fmt.Errorf("building sign request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Vault-Token", token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("calling transit sign: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading sign response: %w", err)
	}

	var signResp transitSignResponse
	if err := json.Unmarshal(respBody, &signResp); err != nil {
		return "", fmt.Errorf("decoding sign response (status %d): %w", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK || signResp.Data.Signature == "" {
		return "", fmt.Errorf("transit sign failed (status %d): %v", resp.StatusCode, signResp.Errors)
	}
	return signResp.Data.Signature, nil
}

type transitVerifyRequest struct {
	Input     string `json:"input"`
	Signature string `json:"signature"`
}

type transitVerifyResponse struct {
	Data struct {
		Valid bool `json:"valid"`
	} `json:"data"`
	Errors []string `json:"errors"`
}

// Verify checks signature against input under keyName. A malformed or
// tampered signature, or a signature over different input, returns
// (false, nil) — a definite negative answer, not an error — matching
// Vault's own transit/verify semantics (a well-formed request that
// legitimately doesn't verify is not a call failure). A genuine call
// failure (network, auth, bad key name) returns a non-nil error.
func (c *Client) Verify(ctx context.Context, keyName string, input []byte, signature string) (bool, error) {
	token, err := c.ensureToken(ctx)
	if err != nil {
		return false, fmt.Errorf("vault auth: %w", err)
	}

	reqBody, err := json.Marshal(transitVerifyRequest{
		Input:     base64.StdEncoding.EncodeToString(input),
		Signature: signature,
	})
	if err != nil {
		return false, fmt.Errorf("marshaling verify request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.addr+"/v1/transit/verify/"+keyName, bytes.NewReader(reqBody))
	if err != nil {
		return false, fmt.Errorf("building verify request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Vault-Token", token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("calling transit verify: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, fmt.Errorf("reading verify response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp transitVerifyResponse
		_ = json.Unmarshal(respBody, &errResp)
		// A malformed/garbage signature (e.g. an unsigned manifest whose
		// signature column is empty or nonsense) is Vault's own
		// invalid-signature 400, not a call failure — treated as a
		// definite "no", same as a well-formed-but-wrong signature.
		if resp.StatusCode == http.StatusBadRequest {
			return false, nil
		}
		return false, fmt.Errorf("transit verify failed (status %d): %v", resp.StatusCode, errResp.Errors)
	}

	var verifyResp transitVerifyResponse
	if err := json.Unmarshal(respBody, &verifyResp); err != nil {
		return false, fmt.Errorf("decoding verify response: %w", err)
	}
	return verifyResp.Data.Valid, nil
}

type kvReadResponse struct {
	Data struct {
		Data map[string]any `json:"data"`
	} `json:"data"`
	Errors []string `json:"errors"`
}

// ReadProviderSecret reads secret/provider/{provider} (KV v2 — Step I's
// own scripts/vault-load-provider-keys.sh writes to this exact path) and
// returns its api_key field. Used by Step J's IssueProviderToken — never
// logs the returned value, only that a read happened.
func (c *Client) ReadProviderSecret(ctx context.Context, provider string) (string, error) {
	token, err := c.ensureToken(ctx)
	if err != nil {
		return "", fmt.Errorf("vault auth: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.addr+"/v1/secret/data/provider/"+provider, nil)
	if err != nil {
		return "", fmt.Errorf("building secret read request: %w", err)
	}
	req.Header.Set("X-Vault-Token", token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("calling secret read: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading secret read response: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return "", fmt.Errorf("no secret stored at secret/provider/%s", provider)
	}
	if resp.StatusCode != http.StatusOK {
		var errResp kvReadResponse
		_ = json.Unmarshal(respBody, &errResp)
		return "", fmt.Errorf("secret read failed (status %d): %v", resp.StatusCode, errResp.Errors)
	}

	var kvResp kvReadResponse
	if err := json.Unmarshal(respBody, &kvResp); err != nil {
		return "", fmt.Errorf("decoding secret read response: %w", err)
	}

	apiKey, ok := kvResp.Data.Data["api_key"].(string)
	if !ok || apiKey == "" {
		return "", fmt.Errorf("secret/provider/%s has no non-empty api_key field", provider)
	}
	return apiKey, nil
}
