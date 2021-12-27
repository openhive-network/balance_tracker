#!/bin/bash

set -e
set -o pipefail

TEST_SCENARIO_PATH="$(pwd)/tests/performance/test_scenarios.jmx"
TEST_RESULT_PATH="$(pwd)/tests/performance/result.jtl"
TEST_REPORT_DIR="$(pwd)/tests/performance/result_report"
TEST_REPORT_PATH="$(pwd)/tests/performance/result_report/index.html"

run_tests() {
    rm $TEST_RESULT_PATH
    jmeter-5.3 -n -t $TEST_SCENARIO_PATH -l $TEST_RESULT_PATH
}

generate_report() {
    rm -rf $TEST_REPORT_DIR
    mkdir $TEST_REPORT_DIR
    jmeter-5.3 -g $TEST_RESULT_PATH -o $TEST_REPORT_DIR
}

view_result() {
    google-chrome $TEST_REPORT_PATH
}

run_tests
generate_report
view_result