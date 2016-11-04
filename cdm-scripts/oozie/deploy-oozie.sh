#!/bin/sh

# Deploy the oozie jobs to production

sh auditcontrol/bin/prod-oozie-auditcontrol-submit.sh
sh filecrusher/bin/prod-oozie-filecrusher-submit.sh
sh filecrusher-cleanup/bin/prod-oozie-filecrusher-cleanup-submit.sh
sh raw-partitioner/bin/prod-oozie-raw-partitioner-submit.sh
sh regular-partitioner/bin/prod-oozie-regular-partitioner-submit.sh
