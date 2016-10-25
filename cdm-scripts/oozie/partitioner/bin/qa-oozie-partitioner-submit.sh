#!/usr/bin/env bash

export OOZIE_URL=https://xrdcldbdu010001.unix.medcity.net:11443/oozie
#http://stackoverflow.com/questions/59895/can-a-bash-script-tell-which-directory-it-is-stored-in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/../cfg/

echo $(pwd)

$(hdfs dfs -mkdir ${QA.PARTITIONER.APP.PATH}/partitioner)
$(hdfs dfs -put workflow.xml coordinator.xml ../lib/ ${QA.PARTITIONER.APP.PATH})

echo "Deploying jobs to qa"

$(oozie job -config adt/qa-adt-partitioner-coordinator.properties -submit)
$(oozie job -config mdm/qa-mdm-partitioner-coordinator.properties -submit)
$(oozie job -config oru/qa-oru-partitioner-coordinator.properties -submit)

echo "Deploy done"