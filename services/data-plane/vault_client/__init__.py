"""vault_client — Phase-04 Step Q: data-plane's OWN Vault access, entirely
independent of control-plane's (services/control-plane/internal/
vaultclient). This is deliberate, not duplication for its own sake:
model_registry's cache must verify a manifest's signature ITSELF before
trusting it, not rely on "it came from etcd, etcd is fed by control-plane,
control-plane already verified it" — a compromised etcd or a bad publish
must not be able to make data-plane route a real request on an invalid
manifest. Defense in depth means data-plane holds its own credential and
does its own crypto call, not "trust the pipe."

Kubernetes auth, not a static Vault token: same reasoning as
control-plane's own vaultclient — data-plane authenticates using its own
pod's projected ServiceAccount token (the cluster's own identity for it),
validated via Vault's Kubernetes auth method against the bound role
scripts/vault-setup-data-plane.sh configures.

VERIFY ONLY: data-plane's Vault policy (data-plane-manifest-verify) grants
transit/verify on model-manifest-signing but NOT transit/sign — even a
fully compromised data-plane pod cannot forge a new signed manifest, only
check existing signatures. Same key, asymmetric capability, matching
control-plane's own read-only-by-design provider-secrets policy shape
from Step I.
"""

import base64
import time

import httpx

SA_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"

# Renewal buffer mirrors control-plane's own vaultclient.Client — re-login
# before the cached token's TTL actually expires, not exactly at it.
_RENEWAL_BUFFER_SECONDS = 30


class VaultError(Exception):
    """A genuine call failure (network, auth, bad key name) — distinct
    from a well-formed verify call that legitimately answers False."""


class Client:
    def __init__(self, addr: str, k8s_role: str, http_client: httpx.AsyncClient) -> None:
        self._addr = addr.rstrip("/")
        self._k8s_role = k8s_role
        self._http = http_client
        self._token: str | None = None
        self._token_expires_at: float = 0.0

    async def _login(self) -> None:
        with open(SA_TOKEN_PATH) as f:
            jwt = f.read().strip()

        resp = await self._http.post(
            f"{self._addr}/v1/auth/kubernetes/login",
            json={"role": self._k8s_role, "jwt": jwt},
        )
        body = resp.json() if resp.content else {}
        auth = body.get("auth") or {}
        client_token = auth.get("client_token")
        if resp.status_code != 200 or not client_token:
            raise VaultError(
                f"vault kubernetes login failed (status {resp.status_code}): {body.get('errors')}"
            )

        self._token = client_token
        lease_duration = auth.get("lease_duration", 0)
        self._token_expires_at = time.monotonic() + lease_duration - _RENEWAL_BUFFER_SECONDS

    async def _ensure_token(self) -> str:
        if self._token is None or time.monotonic() >= self._token_expires_at:
            await self._login()
        assert self._token is not None
        return self._token

    async def verify(self, key_name: str, payload: bytes, signature: str) -> bool:
        """Verifies signature over payload under key_name via Vault
        Transit. Returns False for a well-formed request that legitimately
        doesn't verify (tampered/unsigned/garbage signature) — that is a
        definite negative answer, not a call failure. Raises VaultError
        only for a genuine call failure (network, auth, bad key name).
        """
        token = await self._ensure_token()

        resp = await self._http.post(
            f"{self._addr}/v1/transit/verify/{key_name}",
            json={"input": base64.b64encode(payload).decode("ascii"), "signature": signature},
            headers={"X-Vault-Token": token},
        )

        if resp.status_code == 400:
            # Vault's own invalid-signature response — same "well-formed
            # request, definite no" treatment control-plane's own
            # vaultclient.Client.Verify gives this exact status code.
            return False
        if resp.status_code != 200:
            body = resp.json() if resp.content else {}
            raise VaultError(
                f"transit verify failed (status {resp.status_code}): {body.get('errors')}"
            )

        body = resp.json()
        return bool(body.get("data", {}).get("valid", False))
