#!/bin/sh

function help(){
        echo "-----------------"
        echo "HELP: $0 SnapshotFile"
        echo "-----------------"
        exit 1
}

if [ $# -ne 1 ]
then
        help
fi

file=$1
if [ ! -f $file ]
then
        echo "ERROR: $file not found"
        exit 1
fi
zkDir=/home/xiaoju/tools/zk-server-2181
JAVA_OPTS="$JAVA_OPTS -Djava.ext.dirs=$zkDir:$zkDir/lib"
java $JAVA_OPTS org.apache.zookeeper.server.SnapshotFormatter "$file"
