# Filecrusher cleanup
A weekly Oozie job that removes the `/clone` directories created by the filecrusher

##### Features 
1. Schedule: once per week at 7 AM
2. This script will delete all filecrusher `/clone` directories
3. Sends an email in case of failure.

##### Deployment
1. Put the `coordinator.xml`, `workflow.xml` into an HDFS directory
2. Update the `oozie.coord.application.path=${nameNode}/` property in each prod*.properties files to point to this directory
3. Update the `workflowPath=${nameNode}/` property in each prod*.properties files to point to this directory
4. Update all `/clone` paths to match what has been set in the filecrusher
5. Run `oozie job -config prod-filecrusher-cleanup-coordinator.properties -submit` 
6. Check the Hue Oozie coordinator UI to see that 1 job has been submitted
7. Success, the job have been scheduled and submitted.

##### Notes

##### TODOs
