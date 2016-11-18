#!/bin/sh

export OOZIE_URL=https://xrdclpbdu010001.unix.medcity.net:11443/oozie
#http://stackoverflow.com/questions/59895/can-a-bash-script-tell-which-directory-it-is-stored-in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/../../regular-partitioner/cfg/

oozie job -config hl7_all_segment_data/prod-hl7_all_segment_data-partitioner-coordinator.properties -submit