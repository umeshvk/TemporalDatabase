#!/bin/sh

#hadoop fs -cp /data/alpha/20030115050607/data-orders.dat /tmp/theData.dat
hadoop fs -cp /data/ordersmv.dat /tmp/theData.dat
hadoop fs -ls /tmp/theData.dat
#hive -hiveconf sliceDate='2003-01-19 00:00:00' -f test.hive 
hive -hiveconf sliceDate="$1" -f test.hive 
