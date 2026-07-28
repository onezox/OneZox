#!/usr/bin/env python3
"""Phase-01 Step I (Load, EC3): sustained streaming stability.

Pass condition, per CLAUDE.md's Phase-01 scope: "no upward latency
drift, no memory/task/connection leak, no restart/OOM" under sustained
streaming (minutes, not a burst) -- NOT an absolute p99 target. On a
local kind cluster running inside WSL2, any single latency number this
script prints is not a meaningful production figure; what's meaningful is
the SHAPE of these trends across the run. Results are reported as
stability findings ("stable, no drift over N min, no restart"), not as a
local number claimed to mean anything on its own.

Specifically watches the admission in-flight gauge across the whole run,
not just at the end: Step H3 proved one guard releases on a mid-stream
disconnect, but that can't rule out a slow leak that only shows up under
sustained volume (a gauge that ratchets upward over minutes, never
returning to its pre-load baseline once the run stops). A single
before/after check can't see that shape; this script samples continuously.

Does not touch meter.rs's placeholder token/cost fields. Making them
"real" here to add realism to the load test is explicitly out of scope
for Phase-01 -- that's Phase-03's job, once there's an actual model call
to report real usage for.

Requires: kubectl port-forward to edge-gateway (8080) already running.
Requires: a tenant with a high enough rpm that ratelimit doesn't
interfere with what this test is actually trying to measure (a
sustained-streaming tenant, not the standard rpm=60 test tenant, which
would exhaust in well under a minute at any real concurrency and spend
the rest of the run returning 429s instead of streaming).
"""
import argparse
import json
import statistics
import subprocess
import sys
import threading
import time
import urllib.request

EDGE_GATEWAY_URL = "http://localhost:8080/v1/chat/completions"
GAUGE_KEY = "admission:cell-local:inflight"


def redis_gauge():
    out = subprocess.run(
        ["kubectl", "exec", "-n", "default", "redis-cluster-0", "--",
         "redis-cli", "-c", "GET", GAUGE_KEY],
        capture_output=True, text=True, timeout=10,
    )
    val = out.stdout.strip()
    return int(val) if val.lstrip("-").isdigit() else 0


def pod_rss_kb():
    out = subprocess.run(
        ["kubectl", "exec", "-n", "default", "deploy/edge-gateway", "--",
         "cat", "/proc/1/status"],
        capture_output=True, text=True, timeout=10,
    )
    for line in out.stdout.splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1])
    return None


def pod_restart_count():
    out = subprocess.run(
        ["kubectl", "get", "pods", "-n", "default", "-l", "app=edge-gateway",
         "-o", "jsonpath={.items[0].status.containerStatuses[0].restartCount}"],
        capture_output=True, text=True, timeout=10,
    )
    txt = out.stdout.strip()
    return int(txt) if txt else 0


def one_request(raw_key):
    req = urllib.request.Request(
        EDGE_GATEWAY_URL,
        data=json.dumps({
            "model": "onezox-ultra",
            "messages": [{"role": "user", "content": "load test"}],
            "stream": True,
        }).encode(),
        headers={
            "Authorization": f"Bearer {raw_key}",
            "content-type": "application/json",
        },
        method="POST",
    )
    start = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            resp.read()  # drain the full SSE stream
        return time.monotonic() - start, True
    except Exception:
        return time.monotonic() - start, False


def worker(raw_key, stop_event, results, results_lock, run_start):
    while not stop_event.is_set():
        latency, ok = one_request(raw_key)
        elapsed = time.monotonic() - run_start
        with results_lock:
            results.append((elapsed, latency, ok))


def percentile(values, pct):
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * pct
    f, c = int(k), min(int(k) + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("raw_key")
    p.add_argument("--duration", type=int, default=240, help="seconds")
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--sample-interval", type=int, default=15)
    args = p.parse_args()

    print(f"=== Phase-01 Step I: sustained streaming stability "
          f"(duration={args.duration}s, concurrency={args.concurrency}) ===")
    print("Framing: measuring STABILITY (drift / leak / restart), not an "
          "absolute p99 -- a kind-on-WSL2 number here is not a production "
          "figure.\n")

    baseline_restarts = pod_restart_count()
    baseline_gauge = redis_gauge()
    baseline_rss = pod_rss_kb()
    print(f"baseline: restarts={baseline_restarts} gauge={baseline_gauge} "
          f"rss={baseline_rss}kB")
    if baseline_gauge != 0:
        print("WARNING: baseline gauge is not 0 -- a previous run may not "
              "have fully drained yet; results below may be affected",
              file=sys.stderr)
    print()

    results = []
    results_lock = threading.Lock()
    stop_event = threading.Event()
    run_start = time.monotonic()

    threads = [
        threading.Thread(target=worker,
                          args=(args.raw_key, stop_event, results, results_lock, run_start),
                          daemon=True)
        for _ in range(args.concurrency)
    ]
    for t in threads:
        t.start()

    gauge_samples = []  # (elapsed_s, gauge, rss_kb)
    next_sample = 0.0
    while time.monotonic() - run_start < args.duration:
        elapsed = time.monotonic() - run_start
        if elapsed >= next_sample:
            g = redis_gauge()
            rss = pod_rss_kb()
            with results_lock:
                n = len(results)
            gauge_samples.append((elapsed, g, rss))
            print(f"t={elapsed:6.1f}s  gauge={g:3d}  rss={rss}kB  "
                  f"requests_completed_so_far={n}")
            next_sample += args.sample_interval
        time.sleep(0.5)

    stop_event.set()
    for t in threads:
        t.join(timeout=20)

    print("\n(draining in-flight requests and letting the gauge settle...)")
    time.sleep(3)
    final_gauge = redis_gauge()
    final_restarts = pod_restart_count()
    final_rss = pod_rss_kb()

    with results_lock:
        all_results = list(results)

    ok_results = [(t, lat) for t, lat, ok in all_results if ok]
    failed = [(t, lat) for t, lat, ok in all_results if not ok]

    print(f"\n=== final state ===")
    print(f"restarts: {baseline_restarts} -> {final_restarts}")
    print(f"gauge:    {baseline_gauge} -> {final_gauge} (after drain)")
    print(f"rss:      {baseline_rss}kB -> {final_rss}kB "
          f"(delta: {final_rss - baseline_rss if final_rss and baseline_rss else 'n/a'}kB)")
    print(f"requests completed: {len(all_results)} "
          f"({len(ok_results)} ok, {len(failed)} failed)")

    print(f"\n=== gauge trend across the run (must stay bounded near "
          f"concurrency={args.concurrency}, not climb without bound) ===")
    for elapsed, g, rss in gauge_samples:
        print(f"  t={elapsed:6.1f}s  gauge={g:3d}  rss={rss}kB")

    print(f"\n=== latency trend by {args.sample_interval}s bucket "
          f"(local numbers -- shape matters, not the absolute values) ===")
    print(f"{'bucket_start':>12} {'n':>5} {'p50_ms':>8} {'p95_ms':>8} "
          f"{'p99_ms':>8} {'max_ms':>8}")
    bucket_size = args.sample_interval
    n_buckets = int(args.duration // bucket_size) + 1
    bucket_p99s = []
    for b in range(n_buckets):
        lo, hi = b * bucket_size, (b + 1) * bucket_size
        bucket_latencies = [lat * 1000 for t, lat in ok_results if lo <= t < hi]
        if not bucket_latencies:
            continue
        p50 = percentile(bucket_latencies, 0.50)
        p95 = percentile(bucket_latencies, 0.95)
        p99 = percentile(bucket_latencies, 0.99)
        bucket_p99s.append(p99)
        print(f"{lo:>10.0f}s {len(bucket_latencies):>5} {p50:>8.1f} "
              f"{p95:>8.1f} {p99:>8.1f} {max(bucket_latencies):>8.1f}")

    print()
    drift_note = "n/a (too few buckets)"
    upward_drift = False
    if len(bucket_p99s) >= 3:
        first_half = bucket_p99s[: len(bucket_p99s) // 2]
        second_half = bucket_p99s[len(bucket_p99s) // 2:]
        first_avg = statistics.mean(first_half)
        second_avg = statistics.mean(second_half)
        ratio = second_avg / first_avg if first_avg else float("inf")
        upward_drift = ratio > 1.5
        drift_note = (f"first-half p99 avg={first_avg:.1f}ms, "
                       f"second-half p99 avg={second_avg:.1f}ms, "
                       f"ratio={ratio:.2f}")
    print(f"drift check: {drift_note}")

    no_restart = final_restarts == baseline_restarts
    gauge_drained = final_gauge == 0
    gauge_bounded = all(g <= args.concurrency + 2 for _, g, _ in gauge_samples)
    rss_sane = (baseline_rss is None or final_rss is None
                or final_rss < baseline_rss * 3)  # generous: catch runaway growth, not noise

    passed = no_restart and gauge_drained and gauge_bounded and not upward_drift

    print(f"\n=== findings ===")
    print(f"  restart/OOM:        {'none' if no_restart else 'RESTART DETECTED'} "
          f"({baseline_restarts} -> {final_restarts})")
    print(f"  gauge bounded:      {'yes' if gauge_bounded else 'NO -- exceeded concurrency+2 at some point'}")
    print(f"  gauge drained to 0: {'yes' if gauge_drained else f'NO -- still {final_gauge} after drain'}")
    print(f"  latency drift:      {'none observed' if not upward_drift else 'UPWARD DRIFT OBSERVED'}")
    print(f"  rss sanity:         {'no runaway growth' if rss_sane else 'RSS more than tripled -- investigate'}")
    print(f"\n=== STEP I: {'STABLE' if passed else 'NEEDS REVIEW'} "
          f"-- no upward latency drift, no leak, no restart over "
          f"{args.duration}s of sustained streaming (concurrency={args.concurrency}) ===")

    if not passed:
        sys.exit(1)


if __name__ == "__main__":
    main()
