#!/usr/bin/env bash

#http://stackoverflow.com/questions/59895/can-a-bash-script-tell-which-directory-it-is-stored-in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/../cfg/

echo $(pwd)

$(hdfs dfs -put workflow.xml coordinator.xml ${PROD.FCC.APP.PATH})

echo "Deploying jobs to prod"

$(oozie job -config prod-filecrusher-cleanup-coordinator.properties -submit)

echo "Deploy done"