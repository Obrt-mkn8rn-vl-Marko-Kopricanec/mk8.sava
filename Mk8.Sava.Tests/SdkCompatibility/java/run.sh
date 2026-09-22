#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repository_root=$(cd -- "$script_directory/../../.." && pwd)
dotnet_host=${DOTNET_HOST_PATH:-dotnet}
temporary_root=$(cd -- "${TMPDIR:-/tmp}" && pwd -P)
data_path=$(mktemp -d "$temporary_root/mk8-sava-java-XXXXXX")
runtime_path=$(mktemp -d "$temporary_root/mk8-sava-java-runtime-XXXXXX")
server_log="$data_path/server.log"
server_pid=""
harness_succeeded=0

cleanup() {
    if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
        kill "$server_pid"
        wait "$server_pid" 2>/dev/null || true
    fi

    case "$runtime_path" in
        "$temporary_root"/mk8-sava-java-runtime-*) rm -rf -- "$runtime_path" ;;
        *) echo "Refusing to remove unexpected Java runtime path: $runtime_path" >&2 ;;
    esac

    if [[ "$harness_succeeded" -eq 1 ]]; then
        case "$data_path" in
            "$temporary_root"/mk8-sava-java-*) rm -rf -- "$data_path" ;;
            *) echo "Refusing to remove unexpected compatibility path: $data_path" >&2 ;;
        esac
    else
        echo "Java SDK compatibility failed; retained $data_path for inspection." >&2
        if [[ -f "$server_log" ]]; then
            tail -n 100 "$server_log" >&2
        fi
    fi
}
trap cleanup EXIT INT TERM

jdk_archive="$runtime_path/OpenJDK21U-jdk_x64_linux_hotspot_21.0.12.1_1.tar.gz"
curl \
    --fail \
    --silent \
    --show-error \
    --location \
    'https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.12.1%2B1/OpenJDK21U-jdk_x64_linux_hotspot_21.0.12.1_1.tar.gz' \
    --output "$jdk_archive"
printf '%s  %s\n' \
    'ce79869e1307ed8ee1e2baa86a412b1eb5b75d10a01006d788a6f968bcfaee94' \
    "$jdk_archive" \
    | sha256sum --check --status
mkdir "$runtime_path/jdk"
tar -xzf "$jdk_archive" -C "$runtime_path/jdk" --strip-components=1

maven_archive="$runtime_path/apache-maven-3.9.16-bin.tar.gz"
curl \
    --fail \
    --silent \
    --show-error \
    --location \
    'https://downloads.apache.org/maven/maven-3/3.9.16/binaries/apache-maven-3.9.16-bin.tar.gz' \
    --output "$maven_archive"
printf '%s  %s\n' \
    '831a8591fe20c8243b1dbe7d71e3244f31d1665b0804b2e825e38cbbe5ce0cafb8338851f90780735568773e0a6cd07bbec107cda0b896b008b861075358b6f6' \
    "$maven_archive" \
    | sha512sum --check --status
mkdir "$runtime_path/maven"
tar -xzf "$maven_archive" -C "$runtime_path/maven" --strip-components=1

env \
    JAVA_HOME="$runtime_path/jdk" \
    "$runtime_path/maven/bin/mvn" \
    --batch-mode \
    --no-transfer-progress \
    -Dcompatibility.build.directory="$runtime_path/target" \
    -Dmaven.repo.local="$runtime_path/maven-repository" \
    -f "$script_directory/pom.xml" \
    package

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
    "$runtime_path/jdk/bin/java" -jar "$runtime_path/target/compatibility.jar"

harness_succeeded=1
