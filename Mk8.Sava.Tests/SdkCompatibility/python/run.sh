#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repository_root=$(cd -- "$script_directory/../../.." && pwd)
dotnet_host=${DOTNET_HOST_PATH:-dotnet}
configuration=${MK8_SAVA_TEST_CONFIGURATION:-Debug}
python_host=${PYTHON_HOST_PATH:-python3}
temporary_root=$(cd -- "${TMPDIR:-/tmp}" && pwd -P)
data_path=$(mktemp -d "$temporary_root/mk8-sava-python-XXXXXX")
runtime_path=$(mktemp -d "$temporary_root/mk8-sava-python-runtime-XXXXXX")
source "$script_directory/../host-pair.sh"
harness_succeeded=0

cleanup() {
    stop_sava_hosts

    case "$runtime_path" in
        "$temporary_root"/mk8-sava-python-runtime-*) rm -rf -- "$runtime_path" ;;
        *) echo "Refusing to remove unexpected Python runtime path: $runtime_path" >&2 ;;
    esac

    if [[ "$harness_succeeded" -eq 1 ]]; then
        case "$data_path" in
            "$temporary_root"/mk8-sava-python-*) rm -rf -- "$data_path" ;;
            *) echo "Refusing to remove unexpected compatibility path: $data_path" >&2 ;;
        esac
    else
        echo "Python SDK compatibility failed; retained $data_path for inspection." >&2
        show_sava_host_logs
    fi
}
trap cleanup EXIT INT TERM

pip_wheel="$runtime_path/pip-26.2.1-py3-none-any.whl"
curl \
    --fail \
    --silent \
    --show-error \
    --location \
    'https://files.pythonhosted.org/packages/f3/6e/1736e5b4ae2b778ef2f81c47d797de9f891d4d8acb047a24ca37a60294dd/pip-26.2.1-py3-none-any.whl' \
    --output "$pip_wheel"
printf '%s  %s\n' \
    '71138adf1f4ca900cdb7d289c21b7494329f2332b6d85f0e1c42108c0384ed3e' \
    "$pip_wheel" \
    | sha256sum --check --status

PYTHONPATH="$pip_wheel" "$python_host" -S -m pip install \
    --disable-pip-version-check \
    --quiet \
    --require-hashes \
    --no-cache-dir \
    --target "$runtime_path/site" \
    --requirement "$script_directory/requirements.lock"

if [[ "${MK8_SAVA_TEST_SKIP_BUILD:-0}" != "1" ]]; then
    "$dotnet_host" build "$repository_root/Mk8.Sava.slnx" --no-restore -c "$configuration"
fi

account_name=devstoreaccount1
account_key='Eby8vdM02xNOcqFeqCnf2WmjO1GwSW3eF4J6tq/K1SZFPTOtr/KBHBeksoGMGwBNPajQKDaZhQ=='
start_sava_hosts "$data_path" "$repository_root" "$configuration" "$dotnet_host" "$account_name" "$account_key"

env \
    PYTHONPATH="$runtime_path/site" \
    MK8_SAVA_BLOB_ENDPOINT="$endpoint" \
    MK8_SAVA_ACCOUNT_NAME="$account_name" \
    MK8_SAVA_ACCOUNT_KEY="$account_key" \
    "$python_host" -S "$script_directory/compat.py"

harness_succeeded=1
