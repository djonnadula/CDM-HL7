#!/usr/bin/env bash

export OOZIE_URL=https://xrdcldbdm010001.unix.medcity.net:11443/oozie
#http://stackoverflow.com/questions/59895/can-a-bash-script-tell-which-directory-it-is-stored-in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/../cfg/

echo "Deploying audit control jobs"

echo $(pwd)

$(hdfs dfs -mkdir ${QA.AUDITCONTROL.APP.PATH})
$(hdfs dfs -rm  ${QA.AUDITCONTROL.APP.PATH}/workflow.xml)
$(hdfs dfs -rm  ${QA.AUDITCONTROL.APP.PATH}/coordinator.xml)
$(hdfs dfs -rm -r -f ${QA.AUDITCONTROL.APP.PATH}/lib/)
$(hdfs dfs -put workflow.xml coordinator.xml ../lib/ ${QA.AUDITCONTROL.APP.PATH})

echo "Deploying jobs to qa"

oozie job -config qa-audit-control-coordinator.properties -submit

echo "Deploy done"