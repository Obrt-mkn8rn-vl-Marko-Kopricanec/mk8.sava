#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repository_root=$(cd -- "$script_directory/../../.." && pwd)
dotnet_host=${DOTNET_HOST_PATH:-dotnet}
configuration=${MK8_SAVA_TEST_CONFIGURATION:-Debug}
temporary_root=$(cd -- "${TMPDIR:-/tmp}" && pwd -P)
data_path=$(mktemp -d "$temporary_root/mk8-sava-java-XXXXXX")
runtime_path=$(mktemp -d "$temporary_root/mk8-sava-java-runtime-XXXXXX")
source "$script_directory/../host-pair.sh"
harness_succeeded=0

cleanup() {
    stop_sava_hosts

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
        show_sava_host_logs
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
    "$runtime_path/jdk/bin/java" -jar "$runtime_path/target/compatibility.jar"

harness_succeeded=1
