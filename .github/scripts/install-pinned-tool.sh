#!/usr/bin/env bash
set -euo pipefail
tool=$1
case "${tool}" in
  terraform)
    url=https://releases.hashicorp.com/terraform/1.9.8/terraform_1.9.8_linux_amd64.zip
    sha256=186e0145f5e5f2eb97cbd785bc78f21bae4ef15119349f6ad4fa535b83b10df8
    ;;
  databricks)
    url=https://github.com/databricks/cli/releases/download/v0.299.0/databricks_cli_0.299.0_linux_amd64.zip
    sha256=ea7dd5fe7869489377010d00efed53778c882f112bbb31cfcfeb8d0e97ec0f52
    ;;
  *)
    echo "::error::Unknown tool: ${tool}"
    exit 1
    ;;
esac
bin_dir="${RUNNER_TEMP}/bin"
archive="${RUNNER_TEMP}/${tool}.zip"
mkdir -p "${bin_dir}"
curl -sSfL --retry 3 -o "${archive}" "${url}"
echo "${sha256}  ${archive}" | sha256sum --check --strict
unzip -o -q "${archive}" "${tool}" -d "${bin_dir}"
rm -f "${archive}"
echo "${bin_dir}" >> "${GITHUB_PATH}"
