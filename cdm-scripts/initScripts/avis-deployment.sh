#!/bin/bash
# version 1.0.0 2016-10-23 (YYYY-MM-DD)
#
# description:  Deployment Script for AVIS Project
#

DEPLOYDIR=/hadoop/cdm/deployment/EBA-1015

### END INIT INFO

RECEIVER=/hadoop/cdm/avis/receiver/v-1.0.7
RECEIVERLINK=/hadoop/cdm/avis/receiver/latest

mkdir -p $RECEIVER

mkdir -p $RECEIVER/cfg

mkdir -p $RECEIVER/lib

mkdir -p $RECEIVER/bin

mkdir -p /var/log/cdm/avis/receiver/logs

mkdir -p /var/log/cdm/avis/receiver/lock/subsys

mkdir -p /var/log/cdm/avis/receiver/run

cp -f $DEPLOYDIR/hl7process.jar $RECEIVER/lib

cp -f $DEPLOYDIR/AvisReceiver.properties $RECEIVER

cp -f $DEPLOYDIR/hl7AvisReceiver.sh $RECEIVER/bin

cp -f $DEPLOYDIR/hl7AvisReceiverWatcher.sh $RECEIVER/bin

cp $DEPLOYDIR/log4j.properties $RECEIVER/cfg

ln -fsv $RECEIVER $RECEIVERLINK

chmod -R +x /hadoop/cdm/$RECEIVER/latest
chown -R corpsrvcdmbtch:entrappedhhl7_rw $RECEIVER
chown -R corpsrvcdmbtch:entrappedhhl7_rw $RECEIVERLINK

#------------


PROCESS=/hadoop/cdm/avis/process/v-1.0.7
PROCESSLINK=/hadoop/cdm/avis/process/latest

mkdir -p $PROCESS

mkdir -p $PROCESS/cfg

mkdir -p $PROCESS/lib

mkdir -p $PROCESS/bin

mkdir -p /var/log/cdm/avis/process/logs

mkdir -p /var/log/cdm/avis/process/lock/subsys

mkdir -p /var/log/cdm/avis/process/run

cp -f $DEPLOYDIR/hl7process.jar $PROCESS/lib

cp -f $DEPLOYDIR/AdvancedVent.properties $PROCESS

cp -f $DEPLOYDIR/hl7CdmAvis.sh $PROCESS/bin

cp -f $DEPLOYDIR/hl7CdmAvisWatcher.sh $PROCESS/bin

cp $DEPLOYDIR/log4j.properties $PROCESS/cfg

cp $DEPLOYDIR/cdm-hl7-cfg/* $PROCESS/cfg

ln -fsv $PROCESS $PROCESSLINK

chmod -R +x /hadoop/cdm/$PROCESS/latest
chown -R corpsrvcdmbtch:entrappedhhl7_rw $PROCESS
chown -R corpsrvcdmbtch:entrappedhhl7_rw $PROCESSLINK


#--------------------------


EDW=/hadoop/cdm/avis/edw/v-1.0.7
EDWLINK=/hadoop/cdm/avis/edw/latest

mkdir -p /hadoop/cdm/avis/edw/model
mkdir -p /hadoop/cdm/avis/edw/compiled

mkdir -p $EDW

mkdir -p $EDW/cfg

mkdir -p $EDW/lib

mkdir -p $EDW/bin

mkdir -p /var/log/cdm/avis/edw/logs

mkdir -p /var/log/cdm/avis/edw/lock/subsys

mkdir -p /var/log/cdm/avis/edw/run

cp -f $DEPLOYDIR/hl7process.jar $EDW/lib

cp -f $DEPLOYDIR/AvisEdh.properties $EDW/cfg

cp -f $DEPLOYDIR/avisEdw.sh $EDW/bin

ln -fsv $EDW $EDWLINK

chmod -R +x /hadoop/cdm/$EDW/latest
chown -R corpsrvcdmbtch:entrappedhhl7_rw $EDW
chown -R corpsrvcdmbtch:entrappedhhl7_rw $EDWLINK

chown -R corpsrvcdmbtch:entrappedhhl7_rw /var/log/cdm/avis



