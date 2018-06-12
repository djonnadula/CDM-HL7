#!/bin/bash

FILE=$1
FORMAT=$2

## Email Address Validation
echo Checking Email Addresses
grep -E -o "\b[a-zA-Z0-9.-]+@[a-zA-Z0-9.-]+\.[a-zA-Z0-9.-]+\b"  $FILE > EMAIL_${FORMAT}_FIELD.validation
echo Completed Email Addresses

## SSN Validation
echo Checking SSN Validation  Format 123-45-6789
23-45-6789
grep -E  "[0-9]{3}[-][0-9]{2}[-][0-9]{4}" $FILE > SSN_${FORMAT}_123-45-6789_MESSAGE.validation
echo Completed SSN Validation  Format 1
## SSN Validation
echo Checking SSN Validation  Format 123-45-6789
grep -E -o  "[0-9]{3}[-][0-9]{2}[-][0-9]{4}" $FILE > SSN_${FORMAT}_123-45-6789_FIELD.validation
echo Completed SSN Validation  Format 123-45-6789



## SSN Validation
echo Checking SSN Validation  Format 123 45 6789
grep -E  "[0-9]{3}[ ][0-9]{2}[ ][0-9]{4}" $FILE  > SSN_${FORMAT}_123456789_MESSAGE.validation
echo Completed SSN Validation  Format 123 45 6789

## SSN Validation
echo Checking SSN Validation  Format 123 45 6789
grep -E  "[0-9]{3}[ ][0-9]{2}[ ][0-9]{4}" $FILE  > SSN_${FORMAT}_123456789_FIELD.validation
echo Completed SSN Validation  Format 123 45 6789



## Phone Number Validation
echo Checking Phone Number Validation
grep -o '\^(PID)\|\([0-9]\{3\}\-[0-9]\{3\}\-[0-9]\{4\}\)\|\(([0-9]\{3\})[0-9]\{3\}\-[0-9]\{4\}\)\|\([0-9]\{10\}\)\|\([0-9]\{3\}\s[0-9]\{3\}\s[0-9]\{4\}\)' $FILE > PHONE_NUMBERS_${FORMAT}.validation
echo Completed Number Validation
