#!/bin/sh 

uuid=`uuidgen`
echo $uuid;
endDirInclude=/tmp/${uuid}-last-copy-to-hdfs-dirname; 
startDirExclude=/tmp/${uuid}-last-merge-to-mvdb-dirname; 
cd ${ETL_DIR}
mvn -Dlogback.configurationFile=${BIGDATA_DIR}/configuration/logback.xml exec:java -Dexec.mainClass="com.mvdb.etl.actions.FetchConfigValue" -Dexec.args="--customer alpha --name last-copy-to-hdfs-dirname -outputFile $endDirInclude"
mvn -Dlogback.configurationFile=${BIGDATA_DIR}/configuration/logback.xml exec:java -Dexec.mainClass="com.mvdb.etl.actions.FetchConfigValue" -Dexec.args="--customer alpha --name last-merge-to-mvdb-dirname -outputFile $startDirExclude"
echo "startDirExclude:`cat $startDirExclude`"
echo "endDirInclude:`cat $endDirInclude`"
hadoop jar /home/umesh/work/BigData/mvdb/target/mvdb-0.0.1.jar  /data/alpha `cat $startDirExclude` `cat $endDirInclude`
cd ${BIGDATA_DIR}/usage-details
exit 0
