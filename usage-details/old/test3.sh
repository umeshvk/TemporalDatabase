#!/bin/sh
hadoop fs -lsr hdfs://localhost:9000/data/alpha | awk '{print $8}' | grep 'data\-.*\.dat' | grep "\/[0-9]\{14\}/"
hadoop fs -lsr file:///home/umesh/.mvdb/etl/data/alpha  | awk '{print $8}' | grep 'data\-.*\.dat' | grep "\/[0-9]\{14\}/"
