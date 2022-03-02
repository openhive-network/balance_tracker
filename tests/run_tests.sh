#!/bin/bash

jmeter_v=$1

if [ "$2" = "py" ]; then
    TEST_SCENARIO_PATH="$(pwd)/tests/performance/test_scenarios_py.jmx"
else
    TEST_SCENARIO_PATH="$(pwd)/tests/performance/test_scenarios.jmx"
fi

TEST_RESULT_PATH="$(pwd)/tests/performance/result.jtl"
TEST_REPORT_DIR="$(pwd)/tests/performance/result_report"
TEST_REPORT_PATH="$(pwd)/tests/performance/result_report/index.html"

run_performance_tests() {
    rm $TEST_RESULT_PATH
    jmeter-$jmeter_v -n -t $TEST_SCENARIO_PATH -l $TEST_RESULT_PATH
}

generate_performance_report() {
    rm -rf $TEST_REPORT_DIR
    mkdir $TEST_REPORT_DIR
    jmeter-$jmeter_v -g $TEST_RESULT_PATH -o $TEST_REPORT_DIR
}

view_performance_result() {
    google-chrome $TEST_REPORT_PATH
}

run_performance_tests
generate_performance_report
view_performance_result
