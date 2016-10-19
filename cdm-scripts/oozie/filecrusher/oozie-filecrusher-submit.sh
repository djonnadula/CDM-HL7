#!/usr/bin/env bash

ENV=$1

echo "Deploying jobs to ${ENV}"

oozie job -config hl7_all_proc_rejected/${ENV}-hl7_all_proc_rejected-coordinator.properties -submit
oozie job -config hl7_all_segment_data/${ENV}-hl7_all_segment_data-coordinator.properties -submit
oozie job -config hl7_audit_data/${ENV}-hl7_audit_data-coordinator.properties -submit
oozie job -config hl7_json_data/${ENV}-hl7_json_data-coordinator.properties -submit
oozie job -config nlp_patient_id_data/${ENV}-nlp_patient_id_data-coordinator.properties -submit
oozie job -config raw_adt/${ENV}-raw_adt-coordinator.properties -submit
oozie job -config raw_mdm/${ENV}-raw_mdm-coordinator.properties -submit
oozie job -config raw_oru/${ENV}-raw_oru-coordinator.properties -submit

echo "Deploy successful"