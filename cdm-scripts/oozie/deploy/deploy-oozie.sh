#!/bin/sh

# Deploy the oozie jobs to production
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/../

sh auditcontrol/bin/prod-oozie-auditcontrol-submit.sh
sh filecrusher/bin/prod-oozie-filecrusher-submit.sh
sh raw-partitioner/bin/prod-oozie-raw-partitioner-submit.sh
sh regular-partitioner/bin/prod-oozie-regular-partitioner-submit.sh
