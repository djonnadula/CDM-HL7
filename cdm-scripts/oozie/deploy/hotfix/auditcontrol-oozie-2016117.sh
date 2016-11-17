#!/bin/sh

# Deploy the oozie jobs to production
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/../../auditcontrol/lib/
hdfs dfs -put -f PROD-auditscript_impala.sh ${PROD.AUDITCONTROL.APP.PATH}/lib