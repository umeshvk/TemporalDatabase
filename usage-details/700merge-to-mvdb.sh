#!/bin/sh
cd ${ETL_DIR} 
export LIBJARS=/home/umesh/work/BigData/etl/etl/target/etl-0.0.1.jar
export HADOOP_CLASSPATH=/home/umesh/work/BigData/etl/etl/target/etl-0.0.1.jar
#hadoop jar /home/umesh/work/BigData/mvdb/target/mvdb-0.0.1.jar com.mvdb.platform.action.merge.VersionMerge -libjars ${LIBJARS} /data/alpha 
hadoop jar /home/umesh/work/BigData/mvdb/target/mvdb-0.0.1.jar /data/alpha 
cd ${BIGDATA_DIR}/usage-details
