#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repository_root=$(cd -- "$script_directory/../../.." && pwd)
dotnet_host=${DOTNET_HOST_PATH:-dotnet}
temporary_root=$(cd -- "${TMPDIR:-/tmp}" && pwd -P)
data_path=$(mktemp -d "$temporary_root/mk8-sava-azurite-XXXXXX")
server_log="$data_path/azurite.log"
server_pid=""
harness_succeeded=0

cleanup() {
    if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
        kill "$server_pid"
        wait "$server_pid" 2>/dev/null || true
    fi
    if [[ "$harness_succeeded" -eq 1 ]]; then
        case "$data_path" in
            "$temporary_root"/mk8-sava-azurite-*) rm -rf -- "$data_path" ;;
            *) echo "Refusing to remove unexpected Azurite path: $data_path" >&2 ;;
        esac
    else
        echo "Azurite differential failed; retained $data_path for inspection." >&2
        if [[ -f "$server_log" ]]; then
            tail -n 100 "$server_log" >&2
        fi
    fi
}
trap cleanup EXIT INT TERM

corepack yarn --cwd "$script_directory" install --frozen-lockfile --non-interactive --ignore-engines
"$dotnet_host" build "$repository_root/Mk8.Sava.slnx" --no-restore -c Release

port=$(python3 - <<'PY'
import socket
with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    print(listener.getsockname()[1])
PY
)

account_key='Eby8vdM02xNOcqFeqCnf2WmjO1GwSW3eF4J6tq/K1SZFPTOtr/KBHBeksoGMGwBNPajQKDaZhQ=='
AZURITE_ACCOUNTS="devstoreaccount1:$account_key" \
"$script_directory/node_modules/.bin/azurite-blob" \
    --blobHost 127.0.0.1 \
    --blobPort "$port" \
    --location "$data_path" \
    --disableTelemetry \
    --silent \
    >"$server_log" 2>&1 &
server_pid=$!

ready=0
for _ in $(seq 1 100); do
    if [[ $(curl --silent --output /dev/null --write-out '%{http_code}' \
        "http://127.0.0.1:$port/devstoreaccount1?comp=list") != 000 ]]; then
        ready=1
        break
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
        echo "Azurite exited before becoming ready." >&2
        exit 1
    fi
    sleep 0.1
done
if [[ "$ready" -ne 1 ]]; then
    echo "Azurite did not become ready." >&2
    exit 1
fi

connection_string="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=$account_key;BlobEndpoint=http://127.0.0.1:$port/devstoreaccount1;"
env MK8_SAVA_AZURITE_CONNECTION_STRING="$connection_string" \
    "$dotnet_host" test "$repository_root/Mk8.Sava.Tests/Mk8.Sava.Tests.csproj" \
    --no-build --no-restore -c Release \
    --filter FullyQualifiedName~AzuriteDifferentialTests \
    -v normal

harness_succeeded=1
