// Package promclient is admin-api's own narrow Prometheus boundary —
// instant queries only, for the dashboard's own three headline SLO
// numbers (Phase-05.txt's getDashboardMetrics()). Deliberately NOT the
// official prometheus/client_golang API client: this needs one endpoint
// (/api/v1/query) returning one scalar, and pulling in the full API
// client for that would be the same over-dependency this project has
// declined at every other external boundary (control-plane's own
// etcdclient, provider-gateway's credentials.TokenFetcher, analysis's
// own K8sClient over a typed Argo Rollouts clientset).
//
// Architecture Part R's dashboard is specified against ClickHouse +
// a Redpanda WS live tail; neither exists yet (F6/P13), so this phase
// reads the Prometheus that DOES exist — the Phase-05 plan's own
// "modest but genuinely real this phase" line for the dashboard.
package promclient

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client queries a Prometheus HTTP API.
type Client struct {
	addr string
	http *http.Client
}

func New(addr string) *Client {
	return &Client{
		addr: strings.TrimSuffix(addr, "/"),
		// A dashboard panel must never hang on a slow/absent Prometheus —
		// the rest of the page still renders, this number just reads 0.
		http: &http.Client{Timeout: 5 * time.Second},
	}
}

// Querier is the interface resolvers depend on — FakeQuerier in tests.
type Querier interface {
	// QueryScalar runs an instant query expected to yield at most one
	// sample, and returns its value. See the implementation for why an
	// empty result is (0, nil) rather than an error.
	QueryScalar(ctx context.Context, promQL string) (float64, error)
}

type instantQueryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Value [2]json.RawMessage `json:"value"`
		} `json:"result"`
	} `json:"data"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
}

// QueryScalar returns 0 (not an error) for an empty result vector or a
// NaN sample. Both mean the same real thing here — "no traffic matched
// this query's own window" — and a dashboard that errored out whenever
// the cluster happened to be idle would be strictly less useful than
// one that honestly reports zero. This is the SAME NaN-is-real-and-
// expected situation the canary AnalysisTemplate hit at Step O, handled
// differently on purpose: there, an ambiguous SLO verdict had to fail
// safe and roll back, because promoting on an unknown signal is
// dangerous; here nothing is being decided, a human is just reading a
// number, so degrading to 0 is right. A genuinely malformed query or an
// unreachable Prometheus still returns a real error.
func (c *Client) QueryScalar(ctx context.Context, promQL string) (float64, error) {
	endpoint := c.addr + "/api/v1/query?" + url.Values{"query": {promQL}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, fmt.Errorf("building prometheus request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, fmt.Errorf("querying prometheus: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("prometheus returned HTTP %d", resp.StatusCode)
	}

	var out instantQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, fmt.Errorf("decoding prometheus response: %w", err)
	}
	if out.Status != "success" {
		return 0, fmt.Errorf("prometheus query failed: %s: %s", out.ErrorType, out.Error)
	}
	if len(out.Data.Result) == 0 {
		return 0, nil
	}

	// value is [ <unix_time float>, "<sample as string>" ] — the sample
	// is always JSON-string-encoded, including "NaN"/"+Inf", which is
	// exactly why it can't be decoded straight into a float64.
	var raw string
	if err := json.Unmarshal(out.Data.Result[0].Value[1], &raw); err != nil {
		return 0, fmt.Errorf("decoding prometheus sample: %w", err)
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing prometheus sample %q: %w", raw, err)
	}
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, nil
	}
	return v, nil
}
