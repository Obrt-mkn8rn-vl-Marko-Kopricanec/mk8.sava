#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repository_root=$(cd -- "$script_directory/../.." && pwd)
dotnet_host=${DOTNET_HOST_PATH:-dotnet}
temporary_root=$(cd -- "${TMPDIR:-/tmp}" && pwd -P)
mount_root=$(mktemp -d "$temporary_root/mk8-sava-enospc-XXXXXX")
trap 'rmdir -- "$mount_root"' EXIT

"$dotnet_host" build "$repository_root/Mk8.Sava.slnx" --no-restore

unshare -Urm --propagation private -- bash -c '
    set -euo pipefail
    ulimit -c 0 || true
    mount_root=$1
    dotnet_host=$2
    test_project=$3
    mount -t tmpfs -o size=32m,nosuid,nodev tmpfs "$mount_root"
    trap '\''umount -- "$mount_root"'\'' EXIT
    env MK8_SAVA_ENOSPC_DATA_PATH="$mount_root/data" \
        "$dotnet_host" test "$test_project" \
            --no-build \
            --no-restore \
            --filter FullyQualifiedName~StorageEnospcHarnessTests \
            --logger "console;verbosity=minimal"
' _ "$mount_root" "$dotnet_host" "$repository_root/Mk8.Sava.Tests/Mk8.Sava.Tests.csproj"
