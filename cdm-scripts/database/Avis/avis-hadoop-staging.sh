#!/bin/bash


for f in $(hdfs dfs -ls -R /user/hive/warehouse/hl7/landing_zone=ADHOC-AVIS-DELIMITED | awk '$1 !~ /^d/ && $5 == "0" { print $8 }'); do hdfs dfs -rm -skipTrash "$f"; done


td=$1
ofs=$2
echo "transaction date set to :: $td"
echo "offset set to :: $ofs"

beeline --hiveconf transactiondate=$td --hiveconf offset=$ofs --verbose=true --nullemptystring=true --showHeader=true --showWarnings=true --force=false -u "jdbc:hive2://xrdclpbde010001.unix.medcity.net:10000/hl7;principal=hive/_HOST@HCA.CORPAD.NET" -f /hadoop/cdm/avis/edw/latest/cfg/Avis-Staging-Hadoop.sql

ExitCode=$?
if [ $ExitCode != 0 ];
	then
		echo "Avis Hadoop Staging Failed"
		exit 1
fi

echo "Avis Hadoop Staging Succeeded"
