#!/bin/sh

export BIGDATA_DIR=~/work/BigData
export ETL_DIR=${BIGDATA_DIR}/etl/etl
export MVDB_DIR=${BIGDATA_DIR}/mvdb
#logback file: ~/work/BigData/configuration/logback.xml
cd ${ETL_DIR}; mvn -Dlogback.configurationFile=${BIGDATA_DIR}/configuration/logback.xml exec:java -Dexec.mainClass="com.mvdb.etl.actions.InitCustomerData" -Dexec.args="--customer alpha --batchCount 1  --batchSize 10" ; cd ${BIGDATA_DIR}/usage-details
cd ${ETL_DIR}; mvn -Dlogback.configurationFile=${BIGDATA_DIR}/configuration/logback.xml exec:java -Dexec.mainClass="com.mvdb.etl.actions.ModifyCustomerData" -Dexec.args="" ; cd ${BIGDATA_DIR}/usage-details
cd ${ETL_DIR}; mvn -Dlogback.configurationFile=${BIGDATA_DIR}/configuration/logback.xml exec:java -Dexec.mainClass="com.mvdb.etl.actions.ExtractDBChanges" -Dexec.args="--customer alpha" ; cd ${BIGDATA_DIR}/usage-details
cd ${ETL_DIR}; mvn -Dlogback.configurationFile=${BIGDATA_DIR}/configuration/logback.xml exec:java -Dexec.mainClass="com.mvdb.etl.actions.ScanDBChanges" -Dexec.args="--customer alpha --snapshotDir `cat /tmp/etl.extractdbchanges.directory.txt`" ; cd ${BIGDATA_DIR}/usage-details
