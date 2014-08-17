#!/bin/sh
cd ${ETL_DIR} 
mvn -Dlogback.configurationFile=${BIGDATA_DIR}/configuration/logback.xml exec:java -Dexec.mainClass="com.mvdb.etl.actions.ModifyCustomerData" -Dexec.args="--customerName alpha"
cd ${BIGDATA_DIR}/usage-details

