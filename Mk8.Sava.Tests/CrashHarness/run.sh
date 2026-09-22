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

for scenario in chunk-publication metadata-precommit metadata-postcommit reclamation-delete pack-metadata-precommit pack-metadata-postcommit; do
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

echo "All process-termination recovery scenarios passed."
