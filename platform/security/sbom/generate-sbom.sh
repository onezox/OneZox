#!/usr/bin/env bash
# Generates an SPDX JSON SBOM for a given container image using Syft.
#
# Usage: ./generate-sbom.sh <image-ref> [output-path]
#
# Example:
#   ./generate-sbom.sh onezox-signing-test:phase00 examples/onezox-signing-test-phase00.spdx.json
set -euo pipefail

IMAGE_REF="${1:?Usage: $0 <image-ref> [output-path]}"
OUTPUT_PATH="${2:-$(echo "$IMAGE_REF" | tr '/:' '__').spdx.json}"

syft "$IMAGE_REF" -o spdx-json > "$OUTPUT_PATH"

echo "SBOM written to: $OUTPUT_PATH"
echo "Package count: $(python3 -c "import json; print(len(json.load(open('$OUTPUT_PATH'))['packages']))")"
