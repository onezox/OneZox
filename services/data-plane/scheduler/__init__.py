"""scheduler — Phase-03 Step E: admission and role->model binding.

admit/ protects THIS service's own fleet capacity (Redis-backed in-flight
gauge, mirroring edge-gateway's Phase-01 admission pattern). place/ binds
the fast path's one static worker_ref to a live health signal from
provider-gateway's ProviderHealth RPC (Step A7) — not a registry lookup
(Dependencies.txt F10, Phase-04) and not fallback routing to a different
model (Phase-06).
"""
