#!/bin/sh

cd ${ETL_DIR} 
mvn -Dlogback.configurationFile=${BIGDATA_DIR}/configuration/logback.xml exec:java -Dexec.mainClass="com.mvdb.etl.actions.InitCustomerData" -Dexec.args="--customer alpha --batchCount 1  --batchSize 10 --startDate 20020115050607  --endDate 20030115050607" 
cd ${BIGDATA_DIR}/usage-details

