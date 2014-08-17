#!/bin/sh
header=header.txt
for file in "$@"
do
    cat "$header" "$file" > /tmp/xx.$$
    mv /tmp/xx.$$ "$file"
done
