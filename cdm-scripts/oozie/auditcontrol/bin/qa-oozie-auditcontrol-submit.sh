#!/usr/bin/env bash

export OOZIE_URL=https://xrdcldbdu010001.unix.medcity.net:11443/oozie
#http://stackoverflow.com/questions/59895/can-a-bash-script-tell-which-directory-it-is-stored-in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/../cfg/

echo $(pwd)

$(hdfs dfs -mkdir ${QA.AUDITCONTROL.APP.PATH})
$(hdfs dfs -put workflow.xml coordinator.xml ../lib/ ${QA.AUDITCONTROL.APP.PATH})

echo "Deploying jobs to qa"

$(oozie job -config qa-audit-control-coordinator.properties -submit)

echo "Deploy done"