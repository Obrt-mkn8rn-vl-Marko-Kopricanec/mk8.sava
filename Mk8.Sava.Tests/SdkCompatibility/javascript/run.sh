#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repository_root=$(cd -- "$script_directory/../../.." && pwd)
dotnet_host=${DOTNET_HOST_PATH:-dotnet}
configuration=${MK8_SAVA_TEST_CONFIGURATION:-Debug}
temporary_root=$(cd -- "${TMPDIR:-/tmp}" && pwd -P)
data_path=$(mktemp -d "$temporary_root/mk8-sava-javascript-XXXXXX")
source "$script_directory/../host-pair.sh"
harness_succeeded=0

cleanup() {
    stop_sava_hosts

    if [[ "$harness_succeeded" -eq 1 ]]; then
        case "$data_path" in
            "$temporary_root"/mk8-sava-javascript-*) rm -rf -- "$data_path" ;;
            *) echo "Refusing to remove unexpected compatibility path: $data_path" >&2 ;;
        esac
    else
        echo "JavaScript SDK compatibility failed; retained $data_path for inspection." >&2
        show_sava_host_logs
    fi
}
trap cleanup EXIT INT TERM

corepack yarn --cwd "$script_directory" install --frozen-lockfile --non-interactive
if [[ "${MK8_SAVA_TEST_SKIP_BUILD:-0}" != "1" ]]; then
    "$dotnet_host" build "$repository_root/Mk8.Sava.slnx" --no-restore -c "$configuration"
fi

account_name=devstoreaccount1
account_key='Eby8vdM02xNOcqFeqCnf2WmjO1GwSW3eF4J6tq/K1SZFPTOtr/KBHBeksoGMGwBNPajQKDaZhQ=='
start_sava_hosts "$data_path" "$repository_root" "$configuration" "$dotnet_host" "$account_name" "$account_key"

env \
    MK8_SAVA_BLOB_ENDPOINT="$endpoint" \
    MK8_SAVA_ACCOUNT_NAME="$account_name" \
    MK8_SAVA_ACCOUNT_KEY="$account_key" \
    corepack yarn --cwd "$script_directory" run --silent compat

harness_succeeded=1
