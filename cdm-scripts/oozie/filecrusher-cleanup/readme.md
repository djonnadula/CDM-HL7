# Filecrusher cleanup
A weekly Oozie job that removes the `/clone` directories created by the filecrusher

##### Features 
1. Schedule: once per week at 7 AM
2. This script will delete all filecrusher `/clone` directories
3. Sends an email in case of failure.

##### Deployment
1. Run bin/prod-oozie-filecrusher-cleanup-submit.sh or bin/qa-oozie-filecrusher-cleanup-submit.sh depending on environment


##### Notes

##### TODOs
