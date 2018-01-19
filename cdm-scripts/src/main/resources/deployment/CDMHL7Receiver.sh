#!/bin/bash
# version 1.0.0 2018-01-16 (YYYY-MM-DD)
#
# description:  Deployment Script for CDM HL7 Receiver
#

DEPLOYDIR=/hadoop/cdm/deployment/cdm-hl7
APP=cdm-hl7-receiver
VERSION=$1
RECEIVER=/hadoop/cdm/$APP/$VERSION
RECEIVERLINK=/hadoop/cdm/$APP/latest





mkdir -p $RECEIVER

mkdir -p $RECEIVER/cfg

mkdir -p $RECEIVER/lib

mkdir -p $RECEIVER/bin

mkdir -p /var/log/cdm/$APP/logs

mkdir -p /var/log/cdm/$APP/lock/subsys

mkdir -p /var/log/cdm/$APP/run

cp -f $DEPLOYDIR/hl7process.jar $RECEIVER/lib

cp -f $DEPLOYDIR/Receiver.properties $RECEIVER

cp -f $DEPLOYDIR/hl7Receiver.sh $RECEIVER/bin

cp -f $DEPLOYDIR/hl7ReceiverWatcher.sh $RECEIVER/bin

cp $DEPLOYDIR/log4j.properties $RECEIVER/cfg

ln -fsv $RECEIVER $RECEIVERLINK

chmod -R +x /hadoop/cdm/$RECEIVER/latest
chown -R corpsrvcdmbtch:entrappedhhl7_rw $RECEIVER
chown -R corpsrvcdmbtch:entrappedhhl7_rw $RECEIVERLINK

