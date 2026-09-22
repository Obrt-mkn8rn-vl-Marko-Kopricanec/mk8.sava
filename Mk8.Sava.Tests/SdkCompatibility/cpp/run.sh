#!/usr/bin/env bash
set -euo pipefail
ulimit -c 0

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repository_root=$(cd -- "$script_directory/../../.." && pwd)
dotnet_host=${DOTNET_HOST_PATH:-dotnet}
temporary_root=$(cd -- "${TMPDIR:-/tmp}" && pwd -P)
data_path=$(mktemp -d "$temporary_root/mk8-sava-cpp-XXXXXX")
runtime_path=$(mktemp -d "$temporary_root/mk8-sava-cpp-runtime-XXXXXX")
server_log="$data_path/server.log"
server_pid=""
harness_succeeded=0

cleanup() {
    if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
        kill "$server_pid"
        wait "$server_pid" 2>/dev/null || true
    fi

    case "$runtime_path" in
        "$temporary_root"/mk8-sava-cpp-runtime-*) rm -rf -- "$runtime_path" ;;
        *) echo "Refusing to remove unexpected C++ runtime path: $runtime_path" >&2 ;;
    esac

    if [[ "$harness_succeeded" -eq 1 ]]; then
        case "$data_path" in
            "$temporary_root"/mk8-sava-cpp-*) rm -rf -- "$data_path" ;;
            *) echo "Refusing to remove unexpected compatibility path: $data_path" >&2 ;;
        esac
    else
        echo "C++ SDK compatibility failed; retained $data_path for inspection." >&2
        if [[ -f "$server_log" ]]; then
            tail -n 100 "$server_log" >&2
        fi
    fi
}
trap cleanup EXIT INT TERM

vcpkg_archive="$runtime_path/vcpkg-68112cf89d41fdb70c5910f7581707135be298f4.tar.gz"
curl \
    --fail \
    --silent \
    --show-error \
    --location \
    'https://github.com/microsoft/vcpkg/archive/68112cf89d41fdb70c5910f7581707135be298f4.tar.gz' \
    --output "$vcpkg_archive"
printf '%s  %s\n' \
    'a2b2a1f4f75bb9a4a3b2d143d23674bec116b1fd76af5d1a13121dabcfa6158f' \
    "$vcpkg_archive" \
    | sha256sum --check --status
mkdir "$runtime_path/vcpkg"
tar -xzf "$vcpkg_archive" -C "$runtime_path/vcpkg" --strip-components=1
if ! grep --fixed-strings --line-regexp \
    '  "version-semver": "12.18.0",' \
    "$runtime_path/vcpkg/ports/azure-storage-blobs-cpp/vcpkg.json" \
    >/dev/null; then
    echo "Pinned vcpkg catalog does not contain azure-storage-blobs-cpp 12.18.0." >&2
    exit 1
fi

binary_cache_path="$runtime_path/binary-cache"
mkdir "$binary_cache_path"

pkgconf_archive="$runtime_path/pkgconf-3.0.3.tar.gz"
curl \
    --fail \
    --silent \
    --show-error \
    --location \
    'https://github.com/pkgconf/pkgconf/archive/pkgconf-3.0.3.tar.gz' \
    --output "$pkgconf_archive"
printf '%s  %s\n' \
    '90bb12369d296f2e0bea14832b421c4ba40d442e1519758e6e1e7855afab3149' \
    "$pkgconf_archive" \
    | sha256sum --check --status
mkdir "$runtime_path/pkgconf-source"
tar -xzf "$pkgconf_archive" -C "$runtime_path/pkgconf-source" --strip-components=1
make \
    --silent \
    -C "$runtime_path/pkgconf-source" \
    -f Makefile.lite \
    CC=cc \
    CFLAGS='-O2' \
    SYSTEM_LIBDIR='/lib:/lib/x86_64-linux-gnu:/usr/lib:/usr/lib/x86_64-linux-gnu:/usr/lib64' \
    SYSTEM_INCLUDEDIR='/usr/include' \
    PKG_DEFAULT_PATH='/usr/local/lib/pkgconfig:/usr/lib/x86_64-linux-gnu/pkgconfig:/usr/lib/pkgconfig:/usr/share/pkgconfig'
mkdir "$runtime_path/tool-bin"
ln -s \
    "$runtime_path/pkgconf-source/pkgconf-lite" \
    "$runtime_path/tool-bin/pkg-config"
pkgconf_path="$runtime_path/tool-bin:$PATH"

env VCPKG_DISABLE_METRICS=1 "$runtime_path/vcpkg/bootstrap-vcpkg.sh" -disableMetrics

env \
    PATH="$pkgconf_path" \
    VCPKG_DEFAULT_BINARY_CACHE="$binary_cache_path" \
    VCPKG_DISABLE_METRICS=1 \
    "$runtime_path/vcpkg/vcpkg" install \
    --x-manifest-root="$script_directory" \
    --x-install-root="$runtime_path/installed" \
    --downloads-root="$runtime_path/downloads" \
    --triplet=x64-linux \
    --clean-after-build

cmake_host=$(env VCPKG_DISABLE_METRICS=1 \
    "$runtime_path/vcpkg/vcpkg" fetch cmake \
    --downloads-root="$runtime_path/downloads" \
    | tail -n 1)
ninja_host=$(env VCPKG_DISABLE_METRICS=1 \
    "$runtime_path/vcpkg/vcpkg" fetch ninja \
    --downloads-root="$runtime_path/downloads" \
    | tail -n 1)
"$cmake_host" \
    -S "$script_directory" \
    -B "$runtime_path/build" \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_MAKE_PROGRAM="$ninja_host" \
    -DCMAKE_TOOLCHAIN_FILE="$runtime_path/vcpkg/scripts/buildsystems/vcpkg.cmake" \
    -DVCPKG_INSTALLED_DIR="$runtime_path/installed" \
    -DVCPKG_MANIFEST_MODE=OFF \
    -DVCPKG_TARGET_TRIPLET=x64-linux
"$cmake_host" --build "$runtime_path/build" --parallel

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
    "$runtime_path/build/compatibility"

harness_succeeded=1
