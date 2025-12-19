#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

print_help() {
cat <<-END
  Usage: $0 [option(s)]

  Runs JMeter performance tests for Balance Tracker.

  Options:
    --test-report-dir=PATH                Directory where HTML test report will be generated
    --test-result-path=PATH               File where JTL test result will be generated
    --test-thread-count=NUMBER            Number of threads to be used to run tests (default: 8)
    --test-loop-count=NUMBER              Number of loops to be run during tests (default: 60)
    --backend-port=PORT                   Port used by the backend (default: 3000)
    --backend-host=HOSTNAME               Hostname of backend's host (default: localhost)
    --postgres-host=HOSTNAME              PostgreSQL hostname (default: localhost)
    --postgres-port=PORT                  PostgreSQL port (default: 5432)
    --postgres-user=USERNAME              PostgreSQL user name (default: btracker_owner)
    --postgres-url=URL                    PostgreSQL URL (overrides host/port/user if set)
    --schema=SCHEMA                       Balance Tracker schema name (default: btracker_app)
    --help                                Show this help screen and exit

  Example:
    $0 --backend-host=postgrest-server --postgres-host=haf-instance

END
}

run_tests() {
  test_scenario_path="${PROJECT_ROOT}/tests/performance/test_scenarios.jmx"
  test_result_path="${TEST_RESULT_PATH:-${PROJECT_ROOT}/tests/performance/result.jtl}"
  test_report_dir="${TEST_REPORT_DIR:-${PROJECT_ROOT}/tests/performance/result_report}"
  test_thread_count="${TEST_THREAD_COUNT:-8}"
  test_loop_count="${TEST_LOOP_COUNT:-60}"
  backend_port="${BACKEND_PORT:-3000}"
  backend_host="${BACKEND_HOST:-localhost}"
  postgres_user="${POSTGRES_USER:-btracker_owner}"
  postgres_host="${POSTGRES_HOST:-localhost}"
  postgres_port="${POSTGRES_PORT:-5432}"
  postgres_url="${POSTGRES_URL:-}"
  btracker_schema="${BTRACKER_SCHEMA:-btracker_app}"

  while [ $# -gt 0 ]; do
    case "$1" in
      --test-report-dir=*)
        test_report_dir="${1#*=}"
        ;;
      --test-result-path=*)
        test_result_path="${1#*=}"
        ;;
      --test-thread-count=*)
        test_thread_count="${1#*=}"
        ;;
      --test-loop-count=*)
        test_loop_count="${1#*=}"
        ;;
      --postgres-host=*)
        postgres_host="${1#*=}"
        ;;
      --postgres-port=*)
        postgres_port="${1#*=}"
        ;;
      --postgres-user=*)
        postgres_user="${1#*=}"
        ;;
      --postgres-url=*)
        postgres_url="${1#*=}"
        ;;
      --schema=*)
        btracker_schema="${1#*=}"
        ;;
      --backend-port=*)
        backend_port="${1#*=}"
        ;;
      --backend-host=*)
        backend_host="${1#*=}"
        ;;
      --help|-h)
        print_help
        exit 0
        ;;
      -*)
        echo "Unknown option: $1"
        print_help
        exit 1
        ;;
      *)
        echo "Unknown argument: $1"
        print_help
        exit 2
        ;;
    esac
    shift
  done

  postgres_access="${postgres_url:-postgresql://$postgres_user@$postgres_host:$postgres_port/haf_block_log}"

  echo "Creating performance test indexes..."
  psql -a -v "ON_ERROR_STOP=1" "$postgres_access" -c "SET SEARCH_PATH TO ${btracker_schema};" -c "SELECT ${btracker_schema}.create_btracker_indexes();"

  test_summary_report_path="${test_result_path%jtl}xml"

  rm -f "$test_result_path"
  mkdir -p "${test_result_path%/*}"
  rm -rf "$test_report_dir"
  mkdir -p "$test_report_dir"

  echo "Running JMeter performance tests..."
  echo "  Backend: ${backend_host}:${backend_port}"
  echo "  Threads: ${test_thread_count}, Loops: ${test_loop_count}"
  echo "  Report: ${test_report_dir}"

  jmeter --nongui --testfile "$test_scenario_path" --logfile "$test_result_path" \
    --reportatendofloadtests --reportoutputfolder "$test_report_dir" \
    --jmeterproperty backend.port="$backend_port" --jmeterproperty backend.host="$backend_host" \
    --jmeterproperty thread.count="$test_thread_count" --jmeterproperty loop.count="$test_loop_count" \
    --jmeterproperty summary.report.path="$test_summary_report_path"

  echo "Performance tests completed."
}

run_tests "$@"
