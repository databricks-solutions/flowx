#!/usr/bin/env bash
set -euo pipefail
if ! access_token="$(az account get-access-token --query accessToken --output tsv 2>/dev/null)"; then
  exit 0
fi
IFS=. read -r _ payload _ <<< "${access_token}"
if [[ -z "${payload:-}" ]]; then
  unset access_token
  exit 0
fi
while (( ${#payload} % 4 )); do payload="${payload}="; done
if ! object_id="$(printf '%s' "${payload}" | tr -- '_-' '/+' | base64 --decode 2>/dev/null | jq -r '.oid // empty')"; then
  unset access_token payload
  exit 0
fi
unset access_token payload
if [[ -n "${object_id}" ]]; then
  echo "::add-mask::${object_id}"
fi
