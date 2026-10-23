#!/usr/bin/env bash
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
#
# Install a pinned Terraform release into a directory, checking the zip
# against the checksum pinned below and against the SHA256SUMS file HashiCorp
# publishes for the release. Prints the path of the binary.
#
#   tests/terraform/install-terraform.sh [DIR]      (default: $TMPDIR/terraform-<version>)
#
# Same version and checksum for local runs and for CI (the integration
# workflows call this script), so a run cannot silently pick up a different
# Terraform.

set -euo pipefail

TERRAFORM_VERSION="1.17.0"
# sha256 of terraform_1.17.0_linux_amd64.zip, from
# https://releases.hashicorp.com/terraform/1.17.0/terraform_1.17.0_SHA256SUMS
TERRAFORM_SHA256="c168cab7c637a8769fdb90089ddd19de701bc6ff8aca64dc2e2185fe46f59f67"

dir="${1:-${TMPDIR:-/tmp}/terraform-${TERRAFORM_VERSION}}"
mkdir -p "$dir"
if [[ -x "$dir/terraform" ]] && "$dir/terraform" version | grep -q "Terraform v${TERRAFORM_VERSION}\$"; then
    echo "$dir/terraform"
    exit 0
fi

zip="terraform_${TERRAFORM_VERSION}_linux_amd64.zip"
base="https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}"
curl -sSf -o "$dir/$zip" "$base/$zip"
curl -sSf -o "$dir/SHA256SUMS" "$base/terraform_${TERRAFORM_VERSION}_SHA256SUMS"

published=$(awk -v f="$zip" '$2 == f { print $1 }' "$dir/SHA256SUMS")
actual=$(sha256sum "$dir/$zip" | awk '{ print $1 }')
if [[ "$published" != "$TERRAFORM_SHA256" ]]; then
    echo "error: published checksum for $zip is $published, pinned $TERRAFORM_SHA256" >&2
    exit 1
fi
if [[ "$actual" != "$TERRAFORM_SHA256" ]]; then
    echo "error: $zip has checksum $actual, pinned $TERRAFORM_SHA256" >&2
    exit 1
fi

unzip -o -q "$dir/$zip" -d "$dir"
rm -f "$dir/$zip" "$dir/SHA256SUMS"
echo "$dir/terraform"
