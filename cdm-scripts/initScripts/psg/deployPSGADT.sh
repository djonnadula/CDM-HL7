#!/usr/bin/env bash

echo 'Deploying PSG-ACO-ADT job'

#http://stackoverflow.com/questions/59895/can-a-bash-script-tell-which-directory-it-is-stored-in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR

mkdir -p -v /var/log/cdm/psg-aco-adt/run/
mkdir -p -v /var/log/cdm/psg-aco-adt/lock/subsys/
mkdir -p -v /var/log/cdm/psg-aco-adt/logs/
mkdir -p -v /hadoop/cdm/cfg/psg-aco-adt/
mkdir -p -v /hadoop/cdm/lib/psg-aco-adt/

# Clean up those pesky carriage returns
sed -i 's/\r$//g' PSG_ACO_ADT_Job.sh
sed -i 's/\r$//g' PSGACOADTprocessWatcher.sh

# Chmoddin
chmod -v 755 deployPSGADT.sh
chmod -v 755 PSG_ACO_ADT_Job.sh

# Job stuff
cp -v PSGACOADT.properties /hadoop/cdm/cfg/
cp -v cdm-log4j.properties /hadoop/cdm/cfg/psg-aco-adt/
cp -v PSGACOADTJob.jar /hadoop/cdm/lib/psg-aco-adt/
cp -v PSG_ACO_ADT_Job.sh /hadoop/cdm/bin/

# HDFS stuff
hdfs dfs -mkdir hdfs://nameservice1/user/corpsrvcdmbtch/psg-aco-adt/
hdfs dfs -put Fac_Mnemonic_Roster.txt hdfs://nameservice1/user/corpsrvcdmbtch/psg-aco-adt/
hdfs dfs -put Patient_Roster_PSG_Integral.txt hdfs://nameservice1/user/corpsrvcdmbtch/psg-aco-adt/
hdfs dfs -mkdir hdfs://nameservice1/staging/hl7/checkpoint/psg-aco-adt

echo 'DONE'