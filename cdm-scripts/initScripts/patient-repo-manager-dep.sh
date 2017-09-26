#!/usr/bin/env bash
# version 1.0.0 2016-10-23 (YYYY-MM-DD)
#
# description:  Deployment Script for patient-repositoy Manager service
#

DEPLOYDIR=/hadoop/cdm/deployment/SCRI-Patid-Epic
RELEASDIR=/hadoop/cdm/patient-repositoy/v-1.0.6
LINK=/hadoop/cdm/patient-repository/latest


### END INIT INFO

#mkdir -p $LINK

mkdir -p $RELEASDIR 

mkdir -p $RELEASDIR/cfg

mkdir -p $RELEASDIR/lib

mkdir -p $RELEASDIR/bin

mkdir -p /var/log/cdm/patient-repository/logs

mkdir -p /var/log/cdm/patient-repository/lock/subsys

mkdir -p /var/log/cdm/patient-repository/run


cp -f $DEPLOYDIR/hl7process.jar $RELEASDIR/lib

cp -f $DEPLOYDIR/PatientCacheBuilder.properties $RELEASDIR

cp -f $DEPLOYDIR/hl7PatientCache.sh $RELEASDIR/bin

cp -f $DEPLOYDIR/cdm-hl7-cfg/* $RELEASDIR/cfg

ln -s $RELEASDIR $LINK



