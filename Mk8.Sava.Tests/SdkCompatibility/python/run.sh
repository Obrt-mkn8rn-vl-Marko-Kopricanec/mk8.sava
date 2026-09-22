#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repository_root=$(cd -- "$script_directory/../../.." && pwd)
dotnet_host=${DOTNET_HOST_PATH:-dotnet}
python_host=${PYTHON_HOST_PATH:-python3}
temporary_root=$(cd -- "${TMPDIR:-/tmp}" && pwd -P)
data_path=$(mktemp -d "$temporary_root/mk8-sava-python-XXXXXX")
runtime_path=$(mktemp -d "$temporary_root/mk8-sava-python-runtime-XXXXXX")
server_log="$data_path/server.log"
server_pid=""
harness_succeeded=0

cleanup() {
    if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
        kill "$server_pid"
        wait "$server_pid" 2>/dev/null || true
    fi

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
        if [[ -f "$server_log" ]]; then
            tail -n 100 "$server_log" >&2
        fi
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

"$dotnet_host" build "$repository_root/Mk8.Sava.slnx" --no-restore

port=$("$python_host" -S - <<'PY'
import socket
with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    print(listener.getsockname()[1])
PY
)

account_name=devstoreaccount1
account_key='Eby8vdM02xNOcqFeqCnf2WmjO1GwSW3eF4J6tq/K1SZFPTOtr/KBHBeksoGMGwBNPajQKDaZhQ=='
endpoint="http://127.0.0.1:$port/$account_name"
env \
    ASPNETCORE_URLS="http://127.0.0.1:$port" \
    Sava__DataPath="$data_path" \
    Sava__DefaultAccount="$account_name" \
    Sava__Accounts__devstoreaccount1="$account_key" \
    "$dotnet_host" "$repository_root/Mk8.Sava.API/bin/Debug/net10.0/Mk8.Sava.API.dll" \
    >"$server_log" 2>&1 &
server_pid=$!

ready=0
for _ in $(seq 1 100); do
    if curl --fail --silent "http://127.0.0.1:$port/health/ready" >/dev/null; then
        ready=1
        break
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
        echo "mk8.sava exited before becoming ready." >&2
        exit 1
    fi
    sleep 0.1
done
if [[ "$ready" -ne 1 ]]; then
    echo "mk8.sava did not become ready." >&2
    exit 1
fi

env \
    PYTHONPATH="$runtime_path/site" \
    MK8_SAVA_BLOB_ENDPOINT="$endpoint" \
    MK8_SAVA_ACCOUNT_NAME="$account_name" \
    MK8_SAVA_ACCOUNT_KEY="$account_key" \
    "$python_host" -S "$script_directory/compat.py"

harness_succeeded=1
