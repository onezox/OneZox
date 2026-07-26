# SBOM generation (Syft) — Phase-00

Local Syft installation and a worked example, per Part O/N: SBOMs generated
alongside image signing (`platform/security/cosign/`) in the CI pipeline
sequence `sign artifacts (Sigstore) · SBOM`.

## What's here

- `generate-sbom.sh` — wraps `syft <image> -o spdx-json`, used the same way
  CI would invoke it per built image.
- `examples/onezox-signing-test-phase00.spdx.json` — a real SBOM generated
  against the same trivial Alpine test image built and signed in Step 24
  (`cosign`), proving the two tools work against the same artifact.

## What was verified (Step 25)

```
$ ./generate-sbom.sh onezox-signing-test:phase00 examples/onezox-signing-test-phase00.spdx.json
```

Checked directly against the output, not assumed:
- `spdxVersion: SPDX-2.3`, `dataLicense: CC0-1.0` — valid SPDX document
- `creationInfo.creators` includes `Tool: syft-1.49.0` — real provenance
- 15 packages catalogued, including `musl 1.2.5-r3`, `busybox 1.36.1-r31`,
  `apk-tools 2.14.4-r1`, `alpine-baselayout 3.6.5-r0` — the actual base
  image contents, with real version numbers, not a stub or empty result.

## Local vs. production differences

| | This phase (local) | Production |
|---|---|---|
| **CI automation** | Run manually, once, against a hand-built test image | Generated automatically per build, for every image, as a mandatory pipeline stage (Part N: `... container scan → sign artifacts (Sigstore) · SBOM → Argo CD sync`) — a build that can't produce an SBOM doesn't ship |
| **Signing attestations** | SBOM is a plain JSON file on disk — trustworthy only because you generated it yourself, right now | `cosign attest --predicate <sbom.json> --type spdxjson <image>` signs the SBOM as an in-toto attestation and attaches it to the image itself; `cosign verify-attestation` proves the SBOM matches what CI actually produced, not a file someone swapped in later |
| **Vulnerability scanning** | Not performed this step — SBOM generation only | The SBOM feeds a scanner (Grype — same Anchore family as Syft — or Trivy) continuously matched against a live CVE database; new CVEs get flagged against *already-shipped* images without rebuilding or rescanning, because the SBOM already enumerates exactly what's inside |
| **Storage** | Local file on disk | Attached to the image in the registry (as an attestation) and/or indexed in a central SBOM store queryable across the whole fleet |

The throughline with Step 24: a signed image without an SBOM tells you *who
built it*; an SBOM without a signature tells you *what's claimed to be
inside it* but not whether that claim is trustworthy. Production combines
both — a signed attestation of the SBOM — so neither claim stands alone.
