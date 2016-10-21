#!/usr/bin/env bash

echo "Deploying jobs to qa"

$(oozie job -config hl7_all_proc_rejected/qa-hl7_all_proc_rejected-coordinator.properties -submit)
$(oozie job -config hl7_all_segment_data/qa-hl7_all_segment_data-coordinator.properties -submit)
$(oozie job -config hl7_audit_data/qa-hl7_audit_data-coordinator.properties -submit)
$(oozie job -config hl7_json_data/qa-hl7_json_data-coordinator.properties -submit)
$(oozie job -config nlp_patient_id_data/qa-nlp_patient_id_data-coordinator.properties -submit)
$(oozie job -config raw_adt/qa-raw_adt-coordinator.properties -submit)
$(oozie job -config raw_mdm/qa-raw_mdm-coordinator.properties -submit)
$(oozie job -config raw_oru/qa-raw_oru-coordinator.properties -submit)

echo "Deploy done"