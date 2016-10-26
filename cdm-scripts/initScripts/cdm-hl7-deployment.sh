#!/usr/bin/env bash

# version 1.0.0 2016-10-23 (YYYY-MM-DD)
#
# description:  Deployment Script for HCA CDM HL7 Processing  service
#

### END INIT INFO
export JAVA_HOME=/usr/java/latest

WORKINGDIR=$1
USER=$2
if (( "$#" != 2 )); then
    echo "Required 2 Args
    1. Directory Where Build Happens
    2. User Id for Downloading Sources"
    exit 1
fi

PROJECT='Cdm-HL7'
SOURCE="git clone -b master http://"${USER}"@githubdev.medcity.net/ClinicalReporting/Cdm-HL7.git"
BUILD='mvn -Dhttp.proxyHost=127.0.0.1 -Dhttp.proxyPort=3128 -Dhttps.proxyHost=127.0.0.1 -Dhttps.proxyPort=3128 clean package'

mkdir -p $WORKINGDIR
clean_up(){
rm -rf $WORKINGDIR
}

cd $WORKINGDIR
$SOURCE

STATUS=$?
if [[ ${STATUS} > 0 ]]; then
  echo "Cannot Download Sources.. $PROJECT"
   exit 1;
fi
cd $PROJECT
$BUILD

STATUS=$?
if [[ ${STATUS} > 0 ]]; then
  echo "Build Failed.. $PROJECT"
   exit 1;
fi

# Create Dir needed for Deployment

PID='/var/log/cdm/cdm-hl7/run'
LOCK='/var/log/cdm/cdm-hl7/lock/subsys'
LOGS='/var/log/cdm/cdm-hl7/logs'
mkdir -p $LOGS
mkdir -p $LOCK

SERVICEDIR='/hadoop/cdm/lib'
CONFIGDIR='/hadoop/cdm/cfg/cdm-hl7-config'
BINDIR='/hadoop/cdm/bin'
CDMHL7PROP='/hadoop/cdm/cfg'
PROJECTDIR=${WORKINGDIR}'/'${PROJECT}
EXEC=${PROJECTDIR}'/cdm-scripts/initScripts/*'
CONFIGS=${PROJECTDIR}'/cdm-scripts/templates/*'
PROP=${PROJECTDIR}'/cdm-jobs/src/main/resources/CDMHL7.properties'
ARTIFACT=${PROJECTDIR}'/cdm-jobs/target/hl7process.jar'

mkdir -p $SERVICEDIR
mkdir -p $CONFIGDIR
mkdir -p $BINDIR

mv -f $CONFIGS $CONFIGDIR
STATUS=$?
if [[ ${STATUS} > 0 ]]; then
  echo "Build Failed.. $PROJECT"
  clean_up
   exit 1;
fi

mv -f  $PROP $CDMHL7PROP
STATUS=$?
if [[ ${STATUS} > 0 ]]; then
  echo "Build Failed.. $PROJECT"
  clean_up
   exit 1;
fi

mv -f $EXEC $BINDIR
STATUS=$?
if [[ ${STATUS} > 0 ]]; then
  echo "Build Failed.. $PROJECT"
  clean_up
   exit 1;
fi

mv -f $ARTIFACT $SERVICEDIR
STATUS=$?
if [[ ${STATUS} > 0 ]]; then
  echo "Build Failed.. $PROJECT"
  clean_up
   exit 1;
fi
chmod -R 0774 $LOGS
chmod -R 0774 $PID
chmod -R 0774 $LOCK
chmod -R 0774 $BINDIR
chmod -R 0774 $CDMHL7PROP
chmod -R 0774 $BINDIR
chmod -R 0774 $SERVICEDIR

cd -P ..
clean_up
echo "Build Successful. Configs, Executables are Deployed.
 To Start Service goto /hadoop/cdm/bin and execute hl7Process.sh"


