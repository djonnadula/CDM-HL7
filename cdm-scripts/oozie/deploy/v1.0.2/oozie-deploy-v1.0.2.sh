#!/bin/sh

export OOZIE_URL=https://xrdclpbdu010001.unix.medcity.net:11443/oozie
#http://stackoverflow.com/questions/59895/can-a-bash-script-tell-which-directory-it-is-stored-in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/../../regular-partitioner/cfg/

#Partitioner updates
oozie job -config hl7_all_segment_data/prod-hl7_all_segment_data-partitioner-coordinator.properties -submit

#Filecrusher updates
cd $DIR/../../filecrusher/cfg/
oozie job -config scri_pat_id_json_history/prod-scri_pat_id_json_history-coordinator.properties -submit
oozie job -kill 0000121-161025102156844-oozie-oozi-C
oozie job -config hl7_all_segment_data/prod-hl7_all_segment_data-coordinator.properties -submit
