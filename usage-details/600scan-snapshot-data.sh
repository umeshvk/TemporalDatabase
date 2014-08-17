#!/bin/sh
cd ${ETL_DIR} 
mvn -Dlogback.configurationFile=${BIGDATA_DIR}/configuration/logback.xml exec:java -Dexec.mainClass="com.mvdb.etl.actions.ScanDBChanges" -Dexec.args="--customer alpha --snapshotDir `cat /tmp/etl.extractdbchanges.directory.txt`" ; 
cd ${BIGDATA_DIR}/usage-details
