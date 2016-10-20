#!/usr/bin/env bash

echo "Deploying jobs to prod"

$(oozie job -config adt/prod-adt-partitioner-coordinator.properties -submit)
$(oozie job -config mdm/prod-mdm-partitioner-coordinator.properties -submit)
$(oozie job -config oru/prods-oru-partitioner-coordinator.properties -submit)

echo "Deploy done"