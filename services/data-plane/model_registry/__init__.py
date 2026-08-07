"""model_registry — Phase-04 Step Q: replaces STATIC_MODEL_REF with a
real, signed, versioned manifest binding, fed by an etcd-backed cache
data-plane maintains itself.

Three properties this module exists to guarantee, all adversarially
tested (tests/test_model_registry.py), not merely asserted to hold:

1. INDEPENDENT verification. Every manifest is signature-checked HERE,
   against data-plane's own Vault Transit client (vault_client/), before
   it is ever trusted enough to route a real request — never because "it
   came from etcd" or "control-plane already checked it." A compromised
   etcd, or a bad direct write to it, cannot make data-plane serve on an
   invalid manifest; the same standard Step G already proved at
   control-plane's own serving path, proved again here independently.

2. BYTE-EXACTNESS. The payload verified is built from spec_json exactly
   as received over etcd — never round-tripped through json.loads() and
   re-serialized before verification (that would risk the identical class
   of bug Step E hit with CockroachDB's JSONB reformatting spec_json's
   bytes out from under its own signature). spec_json is parsed ONLY
   after its signature has already verified, and only to pull out
   worker_ref for resolve()'s own return value — never before, and never
   as part of what gets hashed.

3. HOT-PATH SAFETY. resolve() reads a plain in-memory dict — no network
   call, no per-request Vault/etcd round-trip. A verification failure on
   an incoming etcd update never removes or overwrites an already-trusted
   manifest (last-known-good); it only prevents that bad update from ever
   becoming trusted. A model_ref with no verified manifest at all raises
   the typed ManifestNotFound — never a crash, never a hang.
"""

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass
from typing import Protocol

ETCD_PREFIX = "/onezox/"
MANIFESTS_PREFIX = "/onezox/manifests/"
ACTIVE_PREFIX = "/onezox/active/"

SIGNING_KEY_NAME = "model-manifest-signing"

# Re-sync (not just resume-from-last-revision) after any watch failure —
# the simple, unambiguously-correct recovery: a full re-list from etcd
# rather than reasoning about exactly which revision a resumed watch
# would need. Step R's own reconnect test exercises this path directly.
_RECONNECT_BACKOFF_SECONDS = 2.0


class ManifestNotFound(Exception):
    def __init__(self, model_ref: str) -> None:
        self.model_ref = model_ref
        super().__init__(f"no verified manifest for model_ref={model_ref!r}")


def signed_payload(version_id: str, model_ref: str, spec_json: str) -> bytes:
    """MUST match control-plane's own registry.signedPayload exactly
    (services/control-plane/internal/registry/registry.go) — same field
    order, same "|" delimiter, same UTF-8 encoding. A mismatch here would
    make every genuinely-valid manifest fail this independent
    verification, not just tampered ones."""
    return f"{version_id}|{model_ref}|{spec_json}".encode()


@dataclass(frozen=True)
class Manifest:
    version_id: str
    model_ref: str
    spec_json: str
    signature: str
    created_by: str
    created_at: str
    status: str


@dataclass(frozen=True)
class ActivePointer:
    """Phase-05: /onezox/active/{model_ref}'s own shape, grown from a bare
    version_id string (Phase-04) into this small envelope. canary="" means
    no canary in progress — a version_id is a UUID, never legitimately
    empty, so this needs no None/null special-casing anywhere it's read."""

    stable: str
    canary: str = ""
    canary_percent: int = 0


def _parse_active_envelope(value: bytes) -> ActivePointer:
    """Backward-compatible with Phase-04's own bare-string wire format:
    the 5 real providers' active pointers, written by control-plane's
    Step T bootstrap registrations, will not be rewritten into the new
    JSON envelope until something calls PublishActive/a rollout promotion
    against them again (Step L onward) — there is no coordinated one-time
    migration of already-published etcd keys, by design (rewriting them
    directly would itself be an unsigned, out-of-band mutation of what's
    "active," the exact thing EC4 exists to rule out). A value that isn't
    valid JSON is treated as a bare stable version_id (canary="",
    canary_percent=0) — Phase-04's own exact meaning — rather than
    treated as malformed and discarded; this self-heals the moment
    control-plane writes that key again in the new format, with zero
    manual intervention required."""
    try:
        envelope = json.loads(value)
        return ActivePointer(
            stable=envelope.get("stable", ""),
            canary=envelope.get("canary", ""),
            canary_percent=int(envelope.get("canary_percent", 0)),
        )
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError, ValueError):
        return ActivePointer(stable=value.decode())


def _pick_version(pointer: ActivePointer, request_id: str) -> str:
    """Deterministic weighted split: hash(request_id) mod 100 against the
    canary_percent threshold. canary_percent=0 (the Phase-04 default,
    still true for every model_ref no rollout has ever touched) always
    resolves to stable — byte-for-byte the same outcome as before this
    envelope existed at all. Hashing request_id (not a random draw) means
    a given request's own canary/stable placement is reproducible if
    ever needed for debugging, though in practice each request_id is
    unique anyway."""
    if pointer.canary_percent <= 0 or not pointer.canary:
        return pointer.stable
    digest = hashlib.sha256(request_id.encode()).hexdigest()
    bucket = int(digest, 16) % 100
    return pointer.canary if bucket < pointer.canary_percent else pointer.stable


class SignatureVerifier(Protocol):
    async def verify(self, key_name: str, payload: bytes, signature: str) -> bool: ...


class EtcdReader(Protocol):
    """The subset of an etcd client Cache needs — decouples this module
    from aetcd's own concrete types, same reasoning usage_event's _PgPool
    Protocol already established for asyncpg."""

    async def get_prefix(self, key_prefix: bytes): ...
    # aetcd's own watch_prefix is itself async — awaiting it returns the
    # actual async-iterable Watch object, it is not directly iterable on
    # its own (confirmed live, see watch_forever's own comment).
    async def watch_prefix(self, key_prefix: bytes): ...


def _parse_worker_ref(spec_json: str) -> str:
    spec = json.loads(spec_json)
    worker_ref = spec.get("worker_ref")
    if not worker_ref:
        raise ValueError("manifest spec_json has no worker_ref field")
    return worker_ref


class Cache:
    def __init__(self, etcd: EtcdReader, verifier: SignatureVerifier, log: logging.Logger) -> None:
        self._etcd = etcd
        self._verifier = verifier
        self._log = log
        # (model_ref, version_id) -> Manifest, ONLY entries that verified.
        self._manifests: dict[tuple[str, str], Manifest] = {}
        # model_ref -> ActivePointer (stable/canary/canary_percent).
        self._active: dict[str, ActivePointer] = {}

    async def _verify(self, m: Manifest) -> bool:
        """Returns False for BOTH a well-formed "no" from the verifier
        AND a genuine call failure (Vault unreachable, missing/misconfigured
        role, network error) — a bad-update caller (_handle_manifest_kv)
        treats these identically: refuse to trust, log, move on. Crashing
        the whole cache (and therefore the whole boot sequence, since
        sync_once() runs at startup) over a transient Vault problem would
        be exactly the hot-path-safety violation this module exists to
        rule out, just at boot time instead of per-request. Live-caught,
        not theoretical: this exact path crashed data-plane's boot on
        first deploy, before data-plane's own Vault role existed yet."""
        payload = signed_payload(m.version_id, m.model_ref, m.spec_json)
        try:
            return await self._verifier.verify(SIGNING_KEY_NAME, payload, m.signature)
        except Exception as e:
            self._log.warning(
                f"model_registry: verifier call failed (treated as verification "
                f"failure, not a crash) model_ref={m.model_ref} "
                f"version_id={m.version_id} error={e}"
            )
            return False

    async def _handle_manifest_kv(self, model_ref: str, version_id: str, value: bytes) -> None:
        try:
            envelope = json.loads(value)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            self._log.warning(
                f"model_registry: malformed manifest envelope, ignoring "
                f"model_ref={model_ref} version_id={version_id} error={e}"
            )
            return

        m = Manifest(
            version_id=envelope.get("version_id", version_id),
            model_ref=envelope.get("model_ref", model_ref),
            spec_json=envelope.get("spec_json", ""),
            signature=envelope.get("signature", ""),
            created_by=envelope.get("created_by", ""),
            created_at=envelope.get("created_at", ""),
            status=envelope.get("status", ""),
        )

        valid = await self._verify(m)
        if not valid:
            # Last-known-good: a failed verification NEVER overwrites or
            # removes whatever this (model_ref, version_id) already held
            # (if anything) — it just refuses to let the bad update in.
            self._log.warning(
                f"model_registry: signature verification FAILED, refusing to trust "
                f"model_ref={model_ref} version_id={version_id}"
            )
            return

        self._manifests[(model_ref, version_id)] = m
        self._log.info(
            f"model_registry: verified and cached model_ref={model_ref} version_id={version_id}"
        )

    def _handle_active_kv(self, model_ref: str, value: bytes) -> None:
        self._active[model_ref] = _parse_active_envelope(value)

    async def sync_once(self) -> None:
        """Full re-list from etcd — the initial load, and also what a
        reconnect after a watch failure falls back to (simpler and safer
        than reasoning about resuming from a specific revision)."""
        result = await self._etcd.get_prefix(ETCD_PREFIX.encode())
        for kv in result:
            key = kv.key.decode()
            if key.startswith(MANIFESTS_PREFIX):
                rest = key[len(MANIFESTS_PREFIX):]
                model_ref, _, version_id = rest.partition("/")
                if model_ref and version_id:
                    await self._handle_manifest_kv(model_ref, version_id, kv.value)
            elif key.startswith(ACTIVE_PREFIX):
                model_ref = key[len(ACTIVE_PREFIX):]
                if model_ref:
                    self._handle_active_kv(model_ref, kv.value)

    async def watch_forever(self) -> None:
        """Long-running background task (main.py spawns this once at
        startup, after sync_once() has already populated the cache).
        Never raises on its own — a watch failure triggers a full
        sync_once() re-sync and a fresh watch, forever, until the process
        exits."""
        while True:
            try:
                # watch_prefix is itself a coroutine — awaiting it returns
                # the Watch object (which supports __aiter__), it is NOT
                # directly async-iterable on its own. Confirmed live: the
                # first deploy of this code hit
                # "'async for' requires an object with __aiter__ method,
                # got coroutine" in a tight failing loop (caught by the
                # except below, which is why it degraded to noisy retries
                # rather than crashing — but it never actually watched
                # anything until this fix).
                watch = await self._etcd.watch_prefix(ETCD_PREFIX.encode())
                async for event in watch:
                    key = event.kv.key.decode()
                    if event.kind == "DELETE":
                        self._handle_delete(key)
                        continue
                    if key.startswith(MANIFESTS_PREFIX):
                        rest = key[len(MANIFESTS_PREFIX):]
                        model_ref, _, version_id = rest.partition("/")
                        if model_ref and version_id:
                            await self._handle_manifest_kv(model_ref, version_id, event.kv.value)
                    elif key.startswith(ACTIVE_PREFIX):
                        model_ref = key[len(ACTIVE_PREFIX):]
                        if model_ref:
                            self._handle_active_kv(model_ref, event.kv.value)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log.warning(f"model_registry: watch failed, re-syncing error={e}")
                await asyncio.sleep(_RECONNECT_BACKOFF_SECONDS)
                try:
                    await self.sync_once()
                except Exception as sync_err:
                    self._log.warning(
                        f"model_registry: re-sync after watch failure also failed error={sync_err}"
                    )

    def _handle_delete(self, key: str) -> None:
        if key.startswith(MANIFESTS_PREFIX):
            rest = key[len(MANIFESTS_PREFIX):]
            model_ref, _, version_id = rest.partition("/")
            self._manifests.pop((model_ref, version_id), None)
        elif key.startswith(ACTIVE_PREFIX):
            model_ref = key[len(ACTIVE_PREFIX):]
            self._active.pop(model_ref, None)

    def resolve(self, model_ref: str, request_id: str) -> str:
        """Hot-path lookup: pure in-memory dict reads plus one SHA256 hash
        of request_id — still no I/O, no network call. Raises
        ManifestNotFound (typed, caught explicitly at the call site,
        mapped to a clean gRPC status) rather than ever returning a
        placeholder or raising something uncaught.

        request_id is a NEW required parameter (Phase-05 Step K) — it's
        what _pick_version hashes to deterministically split traffic
        between stable and canary when a rollout is in progress
        (canary_percent > 0). When canary_percent is 0 (every model_ref
        Phase-04 ever touched, and every model_ref no rollout has ever
        run against), _pick_version always returns stable — this resolves
        byte-for-byte identically to Phase-04's own behavior."""
        pointer = self._active.get(model_ref)
        if pointer is None:
            raise ManifestNotFound(model_ref)
        version_id = _pick_version(pointer, request_id)
        m = self._manifests.get((model_ref, version_id))
        if m is None:
            # The pointer exists but its target either never arrived or
            # never verified — same clean failure, not a crash. Applies
            # equally whether version_id came from stable or canary: an
            # unverified canary version must fail closed to
            # ManifestNotFound, never silently fall back to stable (that
            # would hide a real verification failure behind a misleadingly
            # successful response instead of surfacing it).
            raise ManifestNotFound(model_ref)
        try:
            return _parse_worker_ref(m.spec_json)
        except (json.JSONDecodeError, ValueError) as e:
            self._log.warning(
                f"model_registry: manifest content unusable model_ref={model_ref} "
                f"version_id={version_id} error={e}"
            )
            raise ManifestNotFound(model_ref) from e
