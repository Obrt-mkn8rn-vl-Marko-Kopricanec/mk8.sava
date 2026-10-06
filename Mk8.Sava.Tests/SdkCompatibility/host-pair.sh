#!/usr/bin/env bash
# Source this helper from SDK process lanes; it never changes their assertions.

sava_application_pid=""
sava_gateway_pid=""

stop_sava_hosts() {
    local child_pid
    local shutdown_deadline
    for child_pid in "$sava_gateway_pid" "$sava_application_pid"; do
        if [[ -n "$child_pid" ]]; then
            if kill -0 "$child_pid" 2>/dev/null; then
                kill "$child_pid" 2>/dev/null || true
                shutdown_deadline=$((SECONDS + 20))
                while ((SECONDS < shutdown_deadline)) && kill -0 "$child_pid" 2>/dev/null; do
                    sleep 0.1
                done
                if kill -0 "$child_pid" 2>/dev/null; then
                    echo "Terminating SDK harness child $child_pid after its shutdown budget." >&2
                    kill -KILL "$child_pid" 2>/dev/null || true
                fi
            fi
            wait "$child_pid" 2>/dev/null || true
        fi
    done
    sava_gateway_pid=""
    sava_application_pid=""
}

wait_for_sava_host() {
    local component=$1
    local address=$2
    local child_pid=$3
    local other_pid=${4:-}
    local startup_deadline=$((SECONDS + 10))
    while ((SECONDS < startup_deadline)); do
        if curl --fail --silent --max-time 1 "$address/health/ready" >/dev/null; then
            return
        fi
        if ! kill -0 "$child_pid" 2>/dev/null; then
            echo "mk8.sava $component exited before becoming ready." >&2
            return 1
        fi
        if [[ -n "$other_pid" ]] && ! kill -0 "$other_pid" 2>/dev/null; then
            echo "mk8.sava Application exited while Gateway was starting." >&2
            return 1
        fi
        sleep 0.1
    done
    echo "mk8.sava $component did not become ready." >&2
    return 1
}

start_sava_hosts() {
    local harness_root=$1
    local solution_root=$2
    local build_configuration=$3
    local dotnet_executable=$4
    local storage_account=$5
    local storage_account_key=$6
    local application_port gateway_port
    local environment_name
    local -a sava_host_environment=(env)
    local access_key_file="$harness_root/application-access.key"

    # Disposable process lanes must not inherit production listeners/policies
    # from the invoking shell. Remove only host configuration, not tool paths.
    while IFS= read -r environment_name; do
        case "${environment_name,,}" in
            sava__*|gateway__*|applicationtransport__*|applicationhosting__*|kestrel__*|aspnetcore_*|dotnet_environment)
                sava_host_environment+=(-u "$environment_name") ;;
        esac
    done < <(compgen -e)

    chmod 700 "$harness_root"
    openssl rand -base64 -out "$access_key_file" 32
    chmod 600 "$access_key_file"
    read -r application_port gateway_port < <("${PYTHON_HOST_PATH:-python3}" -S - <<'PY'
import socket
with socket.socket() as application, socket.socket() as gateway:
    application.bind(("127.0.0.1", 0))
    gateway.bind(("127.0.0.1", 0))
    print(application.getsockname()[1], gateway.getsockname()[1])
PY
)
    local application_address="http://127.0.0.1:$application_port"
    local gateway_address="http://127.0.0.1:$gateway_port"
    local application_endpoint="$application_address/internal/application"
    application_log="$harness_root/application.log"
    server_log="$harness_root/gateway.log"
    # shellcheck disable=SC2034 # The calling SDK lane consumes this shared result.
    endpoint="$gateway_address/$storage_account"

    "${sava_host_environment[@]}" \
        ASPNETCORE_ENVIRONMENT=Production \
        DOTNET_ENVIRONMENT=Production \
        ApplicationTransport__Endpoint="$application_endpoint" \
        ApplicationTransport__AccessKeyFile="$access_key_file" \
        Sava__DataPath="$harness_root/storage" \
        Sava__DefaultAccount="$storage_account" \
        "Sava__Accounts__$storage_account=$storage_account_key" \
        "$dotnet_executable" "$solution_root/Mk8.Sava.Application/bin/$build_configuration/net10.0/Mk8.Sava.Application.dll" \
        >"$application_log" 2>&1 &
    sava_application_pid=$!
    wait_for_sava_host Application "$application_address" "$sava_application_pid"

    "${sava_host_environment[@]}" \
        ASPNETCORE_ENVIRONMENT=Production \
        DOTNET_ENVIRONMENT=Production \
        ASPNETCORE_URLS="$gateway_address" \
        ApplicationTransport__Endpoint="$application_endpoint" \
        ApplicationTransport__AccessKeyFile="$access_key_file" \
        Gateway__StagingPath="$harness_root/gateway-staging" \
        Sava__DefaultAccount="$storage_account" \
        "Sava__Accounts__$storage_account=$storage_account_key" \
        "$dotnet_executable" "$solution_root/Mk8.Sava.Gateway/bin/$build_configuration/net10.0/Mk8.Sava.Gateway.dll" \
        >"$server_log" 2>&1 &
    sava_gateway_pid=$!
    wait_for_sava_host Gateway "$gateway_address" "$sava_gateway_pid" "$sava_application_pid"
}

show_sava_host_logs() {
    local log_file
    for log_file in "${server_log:-}" "${application_log:-}"; do
        if [[ -n "$log_file" && -f "$log_file" ]]; then
            echo "Retained host log: $log_file" >&2
            tail -n 100 "$log_file" >&2
        fi
    done
}
