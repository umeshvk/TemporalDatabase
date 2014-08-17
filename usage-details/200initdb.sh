#!/bin/sh
cd ${ETL_DIR}; 
pwd
mvn -Dlogback.configurationFile=${BIGDATA_DIR}/configuration/logback.xml exec:java -Dexec.mainClass="com.mvdb.etl.actions.InitDB"
cd ${BIGDATA_DIR}/usage-details
pwd
