#!/bin/sh
cd ${ETL_DIR} 
mvn -Dlogback.configurationFile=${BIGDATA_DIR}/configuration/logback.xml exec:java -Dexec.mainClass="com.mvdb.etl.actions.ExtractDBChanges" -Dexec.args="--customer alpha"
cd ${BIGDATA_DIR}/usage-details

