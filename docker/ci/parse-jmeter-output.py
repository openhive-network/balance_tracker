#!/usr/bin/python3
# script that parses Apache Jmeter output and returns 1 if one of the tests failed
import re
import sys

for line in sys.stdin:
    match = re.search('summary =[\s].*Err:[ ]{0,10}([1-9]{1,10})[ ].*',line)
    if match :
        print("Test error(s) detected. Exiting with code 1.")
        sys.exit(1)

print("No test errors detected. Exiting with code 0.")