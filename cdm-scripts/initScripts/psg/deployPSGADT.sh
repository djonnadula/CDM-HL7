#!/usr/bin/env bash

echo 'Deploying PSG-ACO-ADT job'

#http://stackoverflow.com/questions/59895/can-a-bash-script-tell-which-directory-it-is-stored-in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR

# Job stuff
cp -v PSGACOADT.properties /hadoop/cdm/cfg/
cp -v PSGACOADTJob.jar /hadoop/cdm/lib/psg-aco-adt/

# HDFS stuff
hdfs dfs -rmr hdfs://nameservice1/staging/hl7/checkpoint/psg-aco-adt

echo 'Done with filesystem operations'

echo "Starting job..."

cd /hadoop/cdm/bin/
./PSG_ACO_ADT_Job.sh start