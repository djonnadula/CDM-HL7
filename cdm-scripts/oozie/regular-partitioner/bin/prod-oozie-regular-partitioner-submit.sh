#!/usr/bin/env bash

export OOZIE_URL=https://xrdclpbdu010001.unix.medcity.net:11443/oozie
#http://stackoverflow.com/questions/59895/can-a-bash-script-tell-which-directory-it-is-stored-in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/../cfg/

echo $(pwd)

$(hdfs dfs -mkdir ${PROD.REGULAR.PARTITIONER.APP.PATH})
$(hdfs dfs -rm  ${PROD.REGULAR.PARTITIONER.APP.PATH}/workflow.xml)
$(hdfs dfs -rm  ${PROD.REGULAR.PARTITIONER.APP.PATH}/coordinator.xml)
$(hdfs dfs -rm -f  ${PROD.REGULAR.PARTITIONER.APP.PATH}/lib/)
$(hdfs dfs -put workflow.xml coordinator.xml ../lib/ ${PROD.REGULAR.PARTITIONER.APP.PATH})

echo "Deploying jobs to prod"

$(oozie job -config hl7_all_proc_rejected/prod-hl7_all_proc_rejected-partitioner-coordinator.properties -submit)

echo "Deploy done"