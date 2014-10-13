#!/bin/bash
# Prepare Env
DIST_BASE=$(cd `dirname $0`; pwd)/..
# HBASE_LIB_DIR=/opt/cloudera/parcels/CDH-5.1.3-1.cdh5.1.3.p0.12/lib/hbase
HBASE_LIB_DIR=/usr/lib/hbase
TABLE_NAME=table1

## Init HDFS
hadoop fs -rm -r -skipTrash /tmp/IStarPoc/$TABLE_NAME
hadoop fs -mkdir -p /tmp/IStarPoc/$TABLE_NAME/range
hadoop fs -put $DIST_BASE/conf/range.tsv /tmp/$TABLE_NAME/range

## Init HBase Table
hbase shell <<EOF
disable '$TABLE_NAME'
drop '$TABLE_NAME'
create '$TABLE_NAME', {NAME => 'cf', VERSIONS => 1, COMPRESSION=>'SNAPPY'}, SPLITS => ['\x01','\x02','\x03']
count '$TABLE_NAME'
exit
EOF

# Generate HFile
HADOOP_CLASSPATH=$HBASE_LIB_DIR/hbase-protocol-0.98.1-cdh5.1.3.jar:/etc/hbase/conf hadoop jar $DIST_BASE/lib/istarpoc-perftest-0.0.1-SNAPSHOT-job.jar istarpoc.perftest.bulkload.HFileGenerator $DIST_BASE/conf/$TABLE_NAME.yml

# Bulk Load HFile
hadoop fs -chmod -R 777 /tmp/IStarPoc/$TABLE_NAME/hfile
HADOOP_CLASSPATH=`hbase classpath` hadoop jar $HBASE_LIB_DIR/hbase-server-0.98.1-cdh5.1.3.jar completebulkload /tmp/IStarPoc/$TABLE_NAME/hfile $TABLE_NAME
