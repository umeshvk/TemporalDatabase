#!/bin/sh

#pushd .
x=`pwd`
cd $HADOOP_INSTALL
jps
bin/stop-all.sh
sleep 5
bin/hadoop namenode -format
bin/start-all.sh
jps
hadoop fs -ls /
#popd
cd $x

