#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repository_root=$(cd -- "$script_directory/../../.." && pwd)
dotnet_host=${DOTNET_HOST_PATH:-dotnet}
temporary_root=$(cd -- "${TMPDIR:-/tmp}" && pwd -P)
data_path=$(mktemp -d "$temporary_root/mk8-sava-javascript-XXXXXX")
server_log="$data_path/server.log"
server_pid=""
harness_succeeded=0

cleanup() {
    if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
        kill "$server_pid"
        wait "$server_pid" 2>/dev/null || true
    fi

    if [[ "$harness_succeeded" -eq 1 ]]; then
        case "$data_path" in
            "$temporary_root"/mk8-sava-javascript-*) rm -rf -- "$data_path" ;;
            *) echo "Refusing to remove unexpected compatibility path: $data_path" >&2 ;;
        esac
    else
        echo "JavaScript SDK compatibility failed; retained $data_path for inspection." >&2
        if [[ -f "$server_log" ]]; then
            tail -n 100 "$server_log" >&2
        fi
    fi
}
trap cleanup EXIT INT TERM

corepack yarn --cwd "$script_directory" install --frozen-lockfile --non-interactive
"$dotnet_host" build "$repository_root/Mk8.Sava.slnx" --no-restore

port=$(python3 - <<'PY'
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
    MK8_SAVA_BLOB_ENDPOINT="$endpoint" \
    MK8_SAVA_ACCOUNT_NAME="$account_name" \
    MK8_SAVA_ACCOUNT_KEY="$account_key" \
    corepack yarn --cwd "$script_directory" run --silent compat

harness_succeeded=1
