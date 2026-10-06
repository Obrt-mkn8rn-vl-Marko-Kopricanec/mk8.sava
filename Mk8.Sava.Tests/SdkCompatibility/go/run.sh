#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repository_root=$(cd -- "$script_directory/../../.." && pwd)
dotnet_host=${DOTNET_HOST_PATH:-dotnet}
configuration=${MK8_SAVA_TEST_CONFIGURATION:-Debug}
temporary_root=$(cd -- "${TMPDIR:-/tmp}" && pwd -P)
data_path=$(mktemp -d "$temporary_root/mk8-sava-go-XXXXXX")
runtime_path=$(mktemp -d "$temporary_root/mk8-sava-go-runtime-XXXXXX")
source "$script_directory/../host-pair.sh"
harness_succeeded=0

cleanup() {
    stop_sava_hosts

    if [[ -d "$runtime_path/module-cache" ]]; then
        chmod -R u+w "$runtime_path/module-cache" 2>/dev/null || true
    fi
    case "$runtime_path" in
        "$temporary_root"/mk8-sava-go-runtime-*) rm -rf -- "$runtime_path" ;;
        *) echo "Refusing to remove unexpected Go runtime path: $runtime_path" >&2 ;;
    esac

    if [[ "$harness_succeeded" -eq 1 ]]; then
        case "$data_path" in
            "$temporary_root"/mk8-sava-go-*) rm -rf -- "$data_path" ;;
            *) echo "Refusing to remove unexpected compatibility path: $data_path" >&2 ;;
        esac
    else
        echo "Go SDK compatibility failed; retained $data_path for inspection." >&2
        show_sava_host_logs
    fi
}
trap cleanup EXIT INT TERM

go_archive="$runtime_path/go1.27.1.linux-amd64.tar.gz"
curl \
    --fail \
    --silent \
    --show-error \
    --location \
    'https://go.dev/dl/go1.27.1.linux-amd64.tar.gz' \
    --output "$go_archive"
printf '%s  %s\n' \
    '63d339f0da5ab53635a56f2490a7984dfe12dfcff22ad749f63edaf590168445' \
    "$go_archive" \
    | sha256sum --check --status
tar -xzf "$go_archive" -C "$runtime_path"
go_host="$runtime_path/go/bin/go"

env \
    CGO_ENABLED=0 \
    GOCACHE="$runtime_path/build-cache" \
    GOMODCACHE="$runtime_path/module-cache" \
    GOFLAGS='-modcacherw' \
    GOPATH="$runtime_path/gopath" \
    GOPROXY='https://proxy.golang.org,direct' \
    GOSUMDB='sum.golang.org' \
    GOTELEMETRY=off \
    GOTOOLCHAIN=local \
    "$go_host" -C "$script_directory" build -mod=readonly -trimpath -o "$runtime_path/compat" .

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
    "$runtime_path/compat"

harness_succeeded=1
