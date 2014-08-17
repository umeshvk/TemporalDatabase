#!/bin/sh

. ./050initenv.sh
rm -rf ~/.mvdb/hdfs
./075-re-init-hdfs.sh
./100init.sh
./200initdb.sh
./300init-customer-data.sh
./400extract-customer-data.sh
