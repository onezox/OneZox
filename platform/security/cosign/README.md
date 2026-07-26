# Image signing (cosign / Sigstore) — Phase-00

Local development key-pair signing, per Part O: *"Signed images (Sigstore)
and SBOM generation wired into CI ... (admission controller enforcement can
be report-only this phase)."*

## What's here

- `cosign.pub` — the dev signing key's **public** half. Safe to commit;
  this is what anyone (including a future CI job or admission controller)
  needs to verify a signature.

## What's deliberately NOT here

- `cosign.key` (private key) and its password live at
  `.tooling/cosign/cosign.key` / `.tooling/cosign/cosign.password` —
  **local-only, gitignored, never committed**, even though this is a
  throwaway dev key. Committing private key material to git history is a
  bad habit regardless of how "dev-only" the key is; the repo enforces the
  same discipline here that a real key would need.

## How signing/verification were proven to work (Step 24)

Cosign signs registry-addressable images (it pushes the signature as a
separate OCI artifact alongside the image), so verification used a
temporary local OCI registry (`registry:2` on `localhost:5000`), torn down
after the test:

```bash
COSIGN_PASSWORD=$(cat .tooling/cosign/cosign.password) \
  cosign sign --key .tooling/cosign/cosign.key \
    --allow-insecure-registry \
    --signing-config .tooling/cosign/signing_config_no_tlog.json \
    --yes \
    localhost:5000/onezox-signing-test@sha256:...

cosign verify --key platform/security/cosign/cosign.pub \
  --allow-insecure-registry \
  --insecure-ignore-tlog=true \
  localhost:5000/onezox-signing-test@sha256:...
```

Verification was also proven to genuinely fail against an unrelated public
key (wrong-key test, exit code 1) — confirming this isn't a rubber stamp.

`--signing-config .../signing_config_no_tlog.json` exists because cosign
v3.x uploads to the public Rekor transparency log **by default even for
key-based signing** — inappropriate for local dev test images, and this
network's outbound HTTPS to `rekor.sigstore.dev` is additionally
intercepted by what appears to be a TLS-inspecting proxy (the returned
certificate identifies as `*.airtel.com`, not Sigstore's). The config file
is the public Sigstore signing config with `rekorTlogUrls` stripped,
generated per cosign's own suggested command.

## Local key-pair signing vs. production keyless (Fulcio/Rekor)

| | This phase: local key pair | Production: keyless |
|---|---|---|
| Key material | Long-lived asymmetric keypair, generated once, password-protected, stored/rotated by us | **No long-lived private key at all** — an ephemeral keypair is generated fresh at sign time and discarded immediately after |
| Identity | Whoever holds the private key can sign — no binding to *who* or *what* signed | Signer identity comes from **OIDC** (e.g. "this GitHub Actions workflow, this repo, this commit, this ref") |
| Trust anchor | The private key itself — if it leaks, anyone can forge signatures until rotated | **Fulcio** (Sigstore's CA) issues a ~10-minute code-signing certificate binding the OIDC identity to the ephemeral key; nothing long-lived to leak |
| Auditability | None inherent — a signature alone doesn't prove *when* or *by what process* it was created | **Rekor**, a public append-only transparency log, records every signing event; verification can require proof of inclusion, making backdated or hidden signing detectable |
| Network dependency | None — fully offline-capable | Requires live calls to Fulcio (at sign time) and ideally Rekor (at sign and verify time) |
| Verification policy | "Does this match public key X?" | "Was this signed by an ephemeral cert Fulcio issued to identity Y, and is there a Rekor inclusion proof?" |

**Why Phase-00 uses local keys, not keyless:** keyless signing's entire
value proposition is binding a signature to a *real CI identity* — a
specific GitHub Actions workflow run, a specific commit. Phase-00 doesn't
have that CI pipeline actually running signing yet (`ci/` is declared per
Part Q, not wired to real automated builds this phase). A local key pair
proves the **signing and verification mechanics** work correctly —
exactly what this step demonstrated — as the right stepping stone before
wiring identity federation to a real CI system that keyless signing
actually depends on.
