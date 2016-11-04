#!/usr/bin/env bash

export OOZIE_URL=https://xrdclpbdu010001.unix.medcity.net:11443/oozie
#http://stackoverflow.com/questions/59895/can-a-bash-script-tell-which-directory-it-is-stored-in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/../cfg/

echo "Deploying filecrusher cleanup jobs"

echo $(pwd)

$(hdfs dfs -mkdir ${PROD.FCC.APP.PATH})
$(hdfs dfs -rm  ${PROD.FCC.APP.PATH}/workflow.xml)
$(hdfs dfs -rm  ${PROD.FCC.APP.PATH}/coordinator.xml)
$(hdfs dfs -rm -r -f  ${PROD.FCC.APP.PATH}/lib/)
$(hdfs dfs -put workflow.xml coordinator.xml ${PROD.FCC.APP.PATH})

echo "Deploying jobs to prod"

oozie job -config prod-filecrusher-cleanup-coordinator.properties -submit

echo "Deploy done"