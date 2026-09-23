#!/usr/bin/env bash
set -euo pipefail
ulimit -c 0 || true

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repository_root=$(cd -- "$script_directory/../.." && pwd)
dotnet_host=${DOTNET_HOST_PATH:-dotnet}
test_project="$repository_root/Mk8.Sava.Tests/Mk8.Sava.Tests.csproj"
worker_filter='FullyQualifiedName=Mk8.Sava.Tests.StorageCrashHarnessTests.CrashWorker'
validation_filter='FullyQualifiedName=Mk8.Sava.Tests.StorageCrashHarnessTests.ValidateCrashRecovery'

"$dotnet_host" build "$repository_root/Mk8.Sava.slnx" --no-restore

for scenario in chunk-staging-write chunk-publication metadata-precommit metadata-postcommit reclamation-delete pack-record-append pack-metadata-precommit pack-metadata-postcommit; do
    data_path=$(mktemp -d "${TMPDIR:-/tmp}/mk8-sava-${scenario}-XXXXXX")
    echo "Injecting process termination for ${scenario} using ${data_path}"
    if env \
        MK8_SAVA_CRASH_SCENARIO="$scenario" \
        MK8_SAVA_CRASH_DATA_PATH="$data_path" \
        "$dotnet_host" test "$test_project" \
            --no-build \
            --no-restore \
            --filter "$worker_filter" \
            --logger 'console;verbosity=minimal'; then
        echo "The ${scenario} worker unexpectedly exited successfully; retained ${data_path} for inspection." >&2
        exit 1
    fi

    env \
        MK8_SAVA_CRASH_SCENARIO="$scenario" \
        MK8_SAVA_CRASH_DATA_PATH="$data_path" \
        "$dotnet_host" test "$test_project" \
            --no-build \
            --no-restore \
            --filter "$validation_filter" \
            --logger 'console;verbosity=minimal'
done

lease_path=$(mktemp -d "${TMPDIR:-/tmp}/mk8-sava-root-lease-XXXXXX")
lease_filter='FullyQualifiedName=Mk8.Sava.Tests.StorageRootLeaseTests.CrossProcessRootLeaseWorker'
echo "Verifying exclusive data-root ownership across processes using ${lease_path}"
env \
    MK8_SAVA_ROOT_LEASE_PATH="$lease_path" \
    MK8_SAVA_ROOT_LEASE_ROLE=holder \
    "$dotnet_host" test "$test_project" \
        --no-build \
        --no-restore \
        --filter "$lease_filter" \
        --logger 'console;verbosity=minimal' &
holder_pid=$!
ready=0
for ((attempt=0; attempt<100; attempt++)); do
    if [[ -f "$lease_path/holder-ready" ]]; then
        ready=1
        break
    fi
    if ! kill -0 "$holder_pid" 2>/dev/null; then
        break
    fi
    sleep 0.1
done
if ((ready == 0)); then
    touch "$lease_path/release-holder"
    wait "$holder_pid" || true
    echo "The root-lease holder did not become ready." >&2
    exit 1
fi

if ! env \
    MK8_SAVA_ROOT_LEASE_PATH="$lease_path" \
    MK8_SAVA_ROOT_LEASE_ROLE=contender \
    "$dotnet_host" test "$test_project" \
        --no-build \
        --no-restore \
        --filter "$lease_filter" \
        --logger 'console;verbosity=minimal'; then
    touch "$lease_path/release-holder"
    wait "$holder_pid" || true
    echo "The root-lease contender was not rejected." >&2
    exit 1
fi
touch "$lease_path/release-holder"
wait "$holder_pid"

echo "All process-termination recovery and cross-process root-lease scenarios passed."
