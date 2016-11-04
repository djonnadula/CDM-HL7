# Audit Control Query
An Oozie job that runs every 4 hours and sends an email if there are any data discrepancies.

##### Features 
1. Schedule: once every 4 hours
2. Runs a hive script that will check to see if there are any data differences between the different processing steps
3. Sends an email in case of failure.

##### Deployment
1. Run bin/prod-oozie-auditcontrol-submit.sh or bin/qa-oozie-auditcontrol-submit.sh depending on environment

##### Notes

##### TODOs
