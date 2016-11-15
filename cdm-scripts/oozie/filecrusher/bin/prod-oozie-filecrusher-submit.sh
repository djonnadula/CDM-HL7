#!/usr/bin/env bash

export OOZIE_URL=https://xrdclpbdu010001.unix.medcity.net:11443/oozie
#http://stackoverflow.com/questions/59895/can-a-bash-script-tell-which-directory-it-is-stored-in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/../cfg/

echo "Deploying filecrusher jobs"

echo $(pwd)

$(hdfs dfs -mkdir ${PROD.FC.APP.PATH})
$(hdfs dfs -rm  ${PROD.FC.APP.PATH}/workflow.xml)
$(hdfs dfs -rm  ${PROD.FC.APP.PATH}/coordinator.xml)
$(hdfs dfs -rm -r -f  ${PROD.FC.APP.PATH}/lib/)
$(hdfs dfs -put workflow.xml coordinator.xml ../lib/ ${PROD.FC.APP.PATH})

echo "Deploying jobs to prod"

$(oozie job -config hl7_all_proc_rejected/prod-hl7_all_proc_rejected-coordinator.properties -submit)
$(oozie job -config hl7_all_segment_data/prod-hl7_all_segment_data-coordinator.properties -submit)
$(oozie job -config hl7_audit_data/prod-hl7_audit_data-coordinator.properties -submit)
$(oozie job -config hl7_json_data/prod-hl7_json_data-coordinator.properties -submit)
$(oozie job -config nlp_patient_id_data/prod-nlp_patient_id_data-coordinator.properties -submit)
$(oozie job -config raw_adt/prod-raw_adt-coordinator.properties -submit)
$(oozie job -config raw_mdm/prod-raw_mdm-coordinator.properties -submit)
$(oozie job -config raw_oru/prod-raw_oru-coordinator.properties -submit)

echo "Deploy done"