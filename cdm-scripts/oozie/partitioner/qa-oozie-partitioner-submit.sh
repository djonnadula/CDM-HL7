#!/usr/bin/env bash

echo "Deploying jobs to qa"

$(oozie job -config adt/qa-adt-partitioner-coordinator.properties -submit)
$(oozie job -config mdm/qa-mdm-partitioner-coordinator.properties -submit)
$(oozie job -config oru/qa-oru-partitioner-coordinator.properties -submit)

echo "Deploy done"