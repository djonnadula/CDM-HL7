#!/usr/bin/env bash

export OOZIE_URL=https://xrdclpbdu010001.unix.medcity.net:11443/oozie
#http://stackoverflow.com/questions/59895/can-a-bash-script-tell-which-directory-it-is-stored-in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/../cfg/

echo $(pwd)

$(hdfs dfs -mkdir ${PROD.PARTITIONER.APP.PATH})
$(hdfs dfs -put workflow.xml coordinator.xml ../lib/ ${PROD.PARTITIONER.APP.PATH})

echo "Deploying jobs to prod"

$(oozie job -config adt/prod-adt-partitioner-coordinator.properties -submit)
$(oozie job -config mdm/prod-mdm-partitioner-coordinator.properties -submit)
$(oozie job -config oru/prod-oru-partitioner-coordinator.properties -submit)

echo "Deploy done"