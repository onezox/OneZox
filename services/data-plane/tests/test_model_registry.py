"""Unit tests for model_registry — Phase-04 Step Q.

Async calls driven via asyncio.run() inside plain sync test functions,
same pattern test_aggregator.py already established (not worth a new
pytest-asyncio dev dependency for this one file either).

FakeVerifier is a REAL HMAC-based verifier, not a stub that always
answers True — Verify genuinely recomputes and compares, so the
adversarial tests below exercise real rejection logic, not a canned
response. Mirrors control-plane's own Go FakeSigner
(services/control-plane/internal/registry/fake.go) exactly, including the
fact that a tampered payload or garbage signature must NOT verify.
"""

import asyncio
import hashlib
import hmac
import json
import logging
from dataclasses import dataclass, field

from model_registry import Cache, ManifestNotFound, signed_payload


class FakeVerifier:
    def __init__(self) -> None:
        self._key = b"fake-test-signing-key"

    def sign(self, payload: bytes) -> str:
        mac = hmac.new(self._key, payload, hashlib.sha256).hexdigest()
        return f"fake:v1:{mac}"

    async def verify(self, key_name: str, payload: bytes, signature: str) -> bool:
        return hmac.compare_digest(self.sign(payload), signature)


class RaisingVerifier:
    """Simulates a genuine Vault call failure (unreachable, missing role,
    network error) — distinct from a well-formed "no" — for the boot-crash
    regression test below."""

    async def verify(self, key_name: str, payload: bytes, signature: str) -> bool:
        raise RuntimeError("vault kubernetes login failed (status 400): invalid role name")


@dataclass
class _KV:
    key: bytes
    value: bytes


@dataclass
class FakeEtcdReader:
    """In-memory stand-in for aetcd's own client — only get_prefix is
    exercised directly by these unit tests (sync_once's own logic); the
    live watch path is proven against the real etcd cluster, not faked
    here."""

    store: dict[bytes, bytes] = field(default_factory=dict)

    def put(self, key: str, value: bytes) -> None:
        self.store[key.encode()] = value

    async def get_prefix(self, key_prefix: bytes):
        return [_KV(key=k, value=v) for k, v in self.store.items() if k.startswith(key_prefix)]

    async def watch_prefix(self, key_prefix: bytes):
        raise NotImplementedError("not exercised by these unit tests")


def _envelope(version_id: str, model_ref: str, spec_json: str, signature: str) -> bytes:
    return json.dumps(
        {
            "version_id": version_id,
            "model_ref": model_ref,
            "spec_json": spec_json,
            "signature": signature,
            "created_by": "test",
            "created_at": "2026-01-01T00:00:00Z",
            "status": "published",
        }
    ).encode()


def _put_manifest(
    etcd: FakeEtcdReader, version_id: str, model_ref: str, spec_json: str, signature: str
) -> None:
    """Writes both the manifest content key and the active pointer —
    what a real control-plane RegisterModelManifest publish produces."""
    etcd.put(
        f"/onezox/manifests/{model_ref}/{version_id}",
        _envelope(version_id, model_ref, spec_json, signature),
    )
    etcd.put(f"/onezox/active/{model_ref}", version_id.encode())


def _null_logger() -> logging.Logger:
    logger = logging.getLogger("test-model-registry")
    logger.addHandler(logging.NullHandler())
    return logger


def test_resolve_valid_manifest() -> None:
    verifier = FakeVerifier()
    etcd = FakeEtcdReader()

    version_id, model_ref = "v1", "openai"
    spec_json = '{"worker_ref":"openai:gpt-4o-mini"}'
    signature = verifier.sign(signed_payload(version_id, model_ref, spec_json))
    _put_manifest(etcd, version_id, model_ref, spec_json, signature)

    cache = Cache(etcd, verifier, _null_logger())
    asyncio.run(cache.sync_once())

    assert cache.resolve("openai") == "openai:gpt-4o-mini"


def test_resolve_unknown_model_ref_raises_typed_error() -> None:
    cache = Cache(FakeEtcdReader(), FakeVerifier(), _null_logger())
    asyncio.run(cache.sync_once())

    try:
        cache.resolve("does-not-exist")
        raise AssertionError("expected ManifestNotFound")
    except ManifestNotFound as e:
        assert e.model_ref == "does-not-exist"


def test_tampered_manifest_rejected() -> None:
    """Adversarial case (same standard as Step G): a manifest whose
    spec_json was altered after signing must NOT verify, and must NEVER
    become resolvable — not silently loaded."""
    verifier = FakeVerifier()
    etcd = FakeEtcdReader()

    version_id, model_ref = "v1", "openai"
    original_spec = '{"worker_ref":"openai:gpt-4o-mini"}'
    signature = verifier.sign(signed_payload(version_id, model_ref, original_spec))

    # Tampered: the etcd VALUE carries different spec_json than what was
    # actually signed — simulates a compromised etcd or a bad direct
    # write, bypassing control-plane's own signing path entirely.
    tampered_spec = '{"worker_ref":"openai:gpt-4o-EXPENSIVE"}'
    _put_manifest(etcd, version_id, model_ref, tampered_spec, signature)

    cache = Cache(etcd, verifier, _null_logger())
    asyncio.run(cache.sync_once())

    try:
        cache.resolve("openai")
        raise AssertionError("expected ManifestNotFound — tampered manifest must never resolve")
    except ManifestNotFound:
        pass


def test_unsigned_manifest_rejected() -> None:
    """A manifest with an empty/garbage signature (what a direct write
    bypassing control-plane's own signing would produce) must be
    refused."""
    verifier = FakeVerifier()
    etcd = FakeEtcdReader()

    version_id, model_ref = "v1", "openai"
    spec_json = '{"worker_ref":"openai:gpt-4o-mini"}'
    _put_manifest(etcd, version_id, model_ref, spec_json, "")

    cache = Cache(etcd, verifier, _null_logger())
    asyncio.run(cache.sync_once())

    try:
        cache.resolve("openai")
        raise AssertionError("expected ManifestNotFound — unsigned manifest must never resolve")
    except ManifestNotFound:
        pass


def test_garbage_signature_rejected() -> None:
    verifier = FakeVerifier()
    etcd = FakeEtcdReader()

    version_id, model_ref = "v1", "openai"
    spec_json = '{"worker_ref":"openai:gpt-4o-mini"}'
    _put_manifest(etcd, version_id, model_ref, spec_json, "fake:v1:not-a-real-signature")

    cache = Cache(etcd, verifier, _null_logger())
    asyncio.run(cache.sync_once())

    try:
        cache.resolve("openai")
        raise AssertionError("expected ManifestNotFound — garbage signature must never resolve")
    except ManifestNotFound:
        pass


def test_verifier_call_failure_does_not_crash_sync() -> None:
    """Regression test: live-caught on data-plane's very first deploy —
    a genuine Vault call failure (data-plane's own K8s-auth role didn't
    exist yet) during sync_once() propagated as an uncaught exception and
    crashed the whole boot sequence, not just refused that one manifest.
    A Vault outage/misconfiguration must degrade to "this manifest isn't
    trusted" (same as any other verification failure), never take down
    the process — the same hot-path-safety principle applied to boot
    time, not just per-request."""
    etcd = FakeEtcdReader()
    version_id, model_ref = "v1", "openai"
    spec_json = '{"worker_ref":"openai:gpt-4o-mini"}'
    _put_manifest(etcd, version_id, model_ref, spec_json, "some-signature")

    cache = Cache(etcd, RaisingVerifier(), _null_logger())
    asyncio.run(cache.sync_once())  # must not raise

    try:
        cache.resolve("openai")
        raise AssertionError("expected ManifestNotFound — a verify call failure must not trust")
    except ManifestNotFound:
        pass


def test_last_known_good_survives_a_bad_update() -> None:
    """A subsequent invalid update for an ALREADY-trusted (model_ref,
    version_id) key must not evict the good entry — a verification
    failure only refuses the bad update, it never un-trusts what was
    already trusted."""
    verifier = FakeVerifier()
    etcd = FakeEtcdReader()

    version_id, model_ref = "v1", "openai"
    spec_json = '{"worker_ref":"openai:gpt-4o-mini"}'
    signature = verifier.sign(signed_payload(version_id, model_ref, spec_json))
    _put_manifest(etcd, version_id, model_ref, spec_json, signature)

    cache = Cache(etcd, verifier, _null_logger())
    asyncio.run(cache.sync_once())
    assert cache.resolve("openai") == "openai:gpt-4o-mini"

    # Simulate a bad direct write to etcd overwriting the SAME key with
    # tampered content (empty signature this time).
    manifest_key = f"/onezox/manifests/{model_ref}/{version_id}"
    etcd.put(manifest_key, _envelope(version_id, model_ref, spec_json, ""))

    # Directly exercise the incremental-update path (what watch_forever
    # would call per event) rather than sync_once, which would rebuild
    # from scratch — this is what proves an in-place bad update is
    # refused without needing to already be a full watch integration test.
    asyncio.run(cache._handle_manifest_kv(model_ref, version_id, etcd.store[manifest_key.encode()]))

    # Still resolves — last-known-good, the bad update never took effect.
    assert cache.resolve("openai") == "openai:gpt-4o-mini"
