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
CFG=$DEPLOYDIR/cfg
WORKDIR=$DEPLOYDIR/temp

mkdir -p $WORKDIR
mkdir -p $CFG

unzip  -o $DEPLOYDIR/hl7process.jar  **/*.sh -d  $WORKDIR
unzip  -o $DEPLOYDIR/hl7process.jar  **/*.txt -d  $WORKDIR
unzip  -o $DEPLOYDIR/hl7process.jar  **/*.properties -d  $WORKDIR
unzip  -o $DEPLOYDIR/hl7process.jar  **/*.csv -d  $WORKDIR

find $WORKDIR \( -type d -name "$CFG" -prune \) -o \( -type f -name "*.sh" -exec cp -t "$CFG" {}  + \)
find $WORKDIR \( -type d -name "$CFG" -prune \) -o \( -type f -name "*.txt" -exec cp -t "$CFG" {}  + \)
find $WORKDIR \( -type d -name "$CFG" -prune \) -o \( -type f -name "*.properties" -exec cp -t "$CFG" {}  + \)
find $WORKDIR \( -type d -name "$CFG" -prune \) -o \( -type f -name "*.csv" -exec cp -t "$CFG" {}  + \)

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

rm -rf $WORKDIR
rm -rf $CFG

