#!/usr/bin/env bash

export OOZIE_URL=https://xrdcldbdu010001.unix.medcity.net:11443/oozie
#http://stackoverflow.com/questions/59895/can-a-bash-script-tell-which-directory-it-is-stored-in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/../cfg/

echo $(pwd)

$(hdfs dfs -mkdir ${QA.FCC.APP.PATH})
$(hdfs dfs -rm  ${QA.FCC.APP.PATH}/workflow.xml)
$(hdfs dfs -rm  ${QA.FCC.APP.PATH}/coordinator.xml)
$(hdfs dfs -put workflow.xml coordinator.xml ${QA.FCC.APP.PATH})

echo "Deploying jobs to qa"

$(oozie job -config qa-filecrusher-cleanup-coordinator.properties -submit)

echo "Deploy done"