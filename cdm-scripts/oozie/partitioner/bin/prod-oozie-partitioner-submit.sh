#!/usr/bin/env bash

#http://stackoverflow.com/questions/59895/can-a-bash-script-tell-which-directory-it-is-stored-in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/../cfg/

echo "Deploying jobs to prod"

$(oozie job -config adt/prod-adt-partitioner-coordinator.properties -submit)
$(oozie job -config mdm/prod-mdm-partitioner-coordinator.properties -submit)
$(oozie job -config oru/prods-oru-partitioner-coordinator.properties -submit)

echo "Deploy done"