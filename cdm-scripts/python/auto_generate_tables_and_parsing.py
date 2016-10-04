import pypyodbc
import yaml


# TODO: remove common columns from individual create statements
# TODO: add support for qa
# TODO: add command line options



def add_leading_zeros(field):
    """
    This will add leading zeros to a given string. 1 if len > 1, 2 if len <= 1
    :param field: string
    :return: string with correct amount of leading zeros
    """
    if len(field) > 1:
        return '{0}'.format('0' + field + '.')
    else:
        return '{0}'.format('00' + field + '.')


def construct_parsing_field(segment, field):
    """
    Formats the field to have correct leading zeros and concats the field to the end
    :param segment: target segment to be formatted
    :param field: given field
    :return: formatted segment + field
    """
    return add_leading_zeros(segment) + field


def construct_parsing_format(current_seg, field1, field2, field3):
    """
    Creates the MessageTypeSegment parsing format
    :param current_seg: current hl7 segment
    :param field1: segment
    :param field2: component
    :param field3: subcomponent
    :return: formatted segment, component, subcomponent
    """
    splitted_seg = current_seg.split('.')
    if field2 == 'None':
        return '{0}'.format(construct_parsing_field(splitted_seg[1], field1))
    elif field3 == 'None':
        return '{0}|{1}'.format(construct_parsing_field(splitted_seg[1], field1),
                                construct_parsing_field(splitted_seg[2], field2))
    else:
        return '{0}|{1}|{2}'.format(construct_parsing_field(splitted_seg[1], field1),
                                    construct_parsing_field(splitted_seg[2], field2),
                                    construct_parsing_field(splitted_seg[3], field3))


def create_table_names(field1, field2, field3):
    """
    Creates the table name based on how many subcomponents are present
    :param field1: segment
    :param field2: component
    :param field3: subcomponent
    :return: formatted table name
    """
    if field2 == 'None':
        return '{0} String,\n'.format(field1)
    elif field3 == 'None':
        return '{0}_{1} String,\n'.format(field1, field2)
    else:
        return '{0}_{1}_{2} String,\n'.format(field1, field2, field3)


def hl7_table_prefix(segment):
    """
    Prefix for all create table statements
    :param segment: hl7 segment name
    :return: create table prefix statement string
    """
    return '\nCREATE EXTERNAL TABLE hl7_{0}_data ('.format(segment)


def hl7_table_suffix(db):
    """
    The suffix for all create table statements
    :param db: hive database name
    :return: create table statement suffix string
    """
    return 'etl_firstinsert_datetime String' \
           '\n) ' \
           '\nROW FORMAT DELIMITED' \
           '\nFIELDS TERMINATED BY \'|\'' \
           '\nSTORED AS SEQUENCEFILE' \
           '\nLOCATION \'/user/hive/warehouse/{0}/hl7_all_segment_data\';\n'.format(db)


def hl7_view_query(segment):
    """
    Create view query string creator
    :param segment: segment name
    :return: create view statement string
    """
    return '\nCREATE VIEW vw_hl7_{0}_data AS SELECT * FROM hl7_{0}_data WHERE segment_type=\'{1}\';\n'.format(
        segment.lower(), segment.upper())


def main():
    with open("properties.yml", 'r') as stream:
        try:
            yaml_props = yaml.load(stream)
        except yaml.YAMLError as exc:
            print (exc)

    chosen_segments = yaml_props.get('chosen_segments')
    message_type_segments = yaml_props.get('message_type_segments')
    common_columns = 'segment_type String,\nmsh_sending_facility String,\nmsh_msg_control_id String,' \
                     '\npid_medical_record_num String,\npid_medical_record_urn String,\npid_pat_account_num String,'

    message_type_segments_name = 'MessageTypeSegments.csv'
    create_table_prod_name = 'all-segment-tables-create-PROD.txt'
    create_table_qa_name = 'all-segment-tables-create-QA.txt'

    message_type_header = 'MessageType,Segment:Project:OuputFormat&optional-FileName:KafkaOutput,Elements\n' \
                          'MDM,ADHOC:CDI:DELIMITED:HL7CDMCDIQA,010.msh_msg_control_id^004.msh_sending_facility^007.msh_msg_create_date_time^012.txa_unique_document_num^012.txa_unique_document_num|001.entity_id^012.txa_unique_document_num|002.namespace_id^006.txa_origination_date_time^007.txa_transcription_date_time^009.msh_msg_type|001.message_code^009.msh_msg_type|002.trigger_event^018.pid_pat_account_num^003.pid_medical_record_num^003.pid_pat_id_list^003.pid_pat_id_list|001.identifier_num^004.pid_medical_record_urn^004.pid_alt_pat_id^004.pid_alt_pat_id|001.identifier_num^007.pid_birth_date_time^005.pid_pat_name|001.last_name^005.pid_pat_name|002.first_name^008.pid_sex^044.pv1_adm_date_time^045.pv1_dchg_date_time^009.txa_originator_code_name|001.person_id^009.txa_originator_code_name|002.last_name^009.txa_originator_code_name|003.first_name^009.txa_originator_code_name|004.middle_name^009.txa_originator_code_name|005.suffix^009.txa_originator_code_name|006.prefix^009.txa_originator_code_name|007.degree^015.txa_filler_order_num^002.txa_document_type|001.primary_id^002.txa_document_type|002.text^002.txa_document_type|003.name_of_coding_sys^017.txa_document_completion_status^005.obx_observation_value\n' \
                          'ORU,ADHOC:CDI:DELIMITED:HL7CDMCDIQA,010.msh_msg_control_id^004.msh_sending_facility^007.msh_msg_create_date_time^003.obr_filler_order_num^002.obr_placer_order_num^011.obr_specimen_action_code^006.obr_requested_date_time^007.obr_observation_date_time^009.msh_msg_type|001.message_code^009.msh_msg_type|002.trigger_event^018.pid_pat_account_num^003.pid_medical_record_num^003.pid_pat_id_list^003.pid_pat_id_list|001.identifier_num^004.pid_medical_record_urn^004.pid_alt_pat_id^004.pid_alt_pat_id|001.identifier_num^007.pid_birth_date_time^005.pid_pat_name|001.last_name^005.pid_pat_name|002.first_name^008.pid_sex^044.pv1_adm_date_time^045.pv1_dchg_date_time^016.obr_ordering_provider|001.person_id^016.obr_ordering_provider|002.last_name^016.obr_ordering_provider|003.first_name^016.obr_ordering_provider|004.middle_name^016.obr_ordering_provider|005.suffix^016.obr_ordering_provider|006.prefix^016.obr_ordering_provider|007.degree^025.obr_result_status^004.obr_universal_service_id|001.identifier^004.obr_universal_service_id|002.text^004.obr_universal_service_id|003.name_of_coding_sys^011.obx_observation_result_status^005.obx_observation_value\n' \
                          'MDM,ADHOC:SCRIPATID:JSON&mdm_key_names.csv^DELIMITED:HL7CDMSCRIPATIDJSONQA^HL7CDMSCRIPATIDDELIMQA,010.msh_msg_control_id^004.msh_sending_facility^006.txa_origination_date_time^007.txa_transcription_date_time^009.msh_msg_type|001.message_code^009.msh_msg_type|002.trigger_event^018.pid_pat_account_num^003.pid_medical_record_num^003.pid_pat_id_list^004.pid_medical_record_urn^004.pid_alt_pat_id^007.pid_birth_date_time^005.pid_pat_name|001.last_name^005.pid_pat_name|002.first_name^008.pid_sex^044.pv1_adm_date_time^045.pv1_dchg_date_time^009.txa_originator_code_name|001.person_id^009.txa_originator_code_name|002.last_name^009.txa_originator_code_name|003.first_name^009.txa_originator_code_name|004.middle_name^009.txa_originator_code_name|005.suffix^009.txa_originator_code_name|006.prefix^009.txa_originator_code_name|007.degree^015.txa_filler_order_num^002.txa_document_type|001.primary_id^002.txa_document_type|002.text^002.txa_document_type|003.name_of_coding_sys^017.txa_document_completion_status^005.obx_observation_value\n'

    zsegments = 'ADT,ZCD,001.zcd_set_id^002.zcd_query_data^002.zcd_query_data|001.mnem^002.zcd_query_data|002.text^002.zcd_query_data|003.response\n' \
                'ADT,ZCN,001.zcn_set_id^002.zcn_collection_note_date_time^003.zcn_collection_note_user^004.zcn_collection_note_text\n' \
                'ADT,ZCS,001.zcs_patient_employer^002.zcs_patient_employer_address^002.zcs_patient_employer_address|001.street_address1^002.zcs_patient_employer_address|002.street_address2^002.zcs_patient_employer_address|003.city^002.zcs_patient_employer_address|004.state_province^002.zcs_patient_employer_address|005.zip_postal_code^003.zcs_patient_employment_status^004.zcs_patient_e_mail_address^005.zcs_guarantor_e_mail_address^006.zcs_patient_accounting_facility_id\n' \
                'ADT,ZEX,001.zex_patient_death_date_time^002.zex_patient_death_ind\n' \
                'ADT,ZIN,001.zin_set_id^002.zin_insured_id^002.zin_insured_id|001.zin_bed_days_information^002.zin_insured_id|002.zin_bed_days_information^002.zin_insured_id|003.zin_bed_days_information^002.zin_insured_id|004.zin_bed_days_information^003.zin_insurance_mail_to_name^004.zin_insurance_verification_ind^005.zin_insurance_verification_date^005.zin_insurance_verification_date|001.zin_denied_days_information^005.zin_insurance_verification_date|002.zin_denied_days_information^006.zin_insurance_verified_by^007.zin_approved_length_of_stay^008.zin_authorization_precertification_required^009.zin_authorization_type^010.zin_authorized_by^011.zin_authorization_phone^012.zin_referral_num^013.zin_insurance_plan_id\n' \
                'ADT,ZM1,001.zm1_set_id^002.zm1_insurance^003.zm1_query_data^003.zm1_query_data|001.mnem^003.zm1_query_data|002.text^003.zm1_query_data|003.response\n' \
                'ADT,ZMR,001.zmr_set_id^002.zmr_med_rec_num\n' \
                'ADT,ZSU,001.zsu_event_type^002.zsu_patient_prior_unit_num\n' \
                'ADT,ZUR,001.zur_set_id^002.zur_information\n' \
                'ADT,ZRQ,001.zrq_set_id^002.zrq_data_type^003.zrq_external_id^004.zrq_sub_id^005.zrq_item_value\n' \
                'ADT,ZID,001.zid_surviving_id^002.zid_non_surviving_id\n' \
                'ADT,ZPE,002.zpe_record_id^003.zpe_transfer_priority^004.zpe_bed_type^005.zpe_ordering_provider^006.zpe_bed_request_status^019.zpe_reason^020.zpe_comment\n' \
                'ADT,ZG1,001.zg1_fpl_effective_from_date^002.zg1_fpl_effective_to_date^003.zg1_fpl_pct^004.zg1_guarantor_service_area\n' \
                'ADT,ZMP,001.zmp_set_id^002.zmp_black_lung_benefit^003.zmp_black_lung_benefit_date^004.zmp_service_paid_by_government_program^005.zmp_va_authorization^006.zmp_accident^007.zmp_work_related_accident^008.zmp_injury_illness_date^009.zmp_worker_comp_policy_num^010.zmp_worker_comp_address^010.zmp_worker_comp_address|001.street_address1^010.zmp_worker_comp_address|002.street_address2^010.zmp_worker_comp_address|003.city^010.zmp_worker_comp_address|004.state_province^010.zmp_worker_comp_address|005.zip_postal_code^011.zmp_accident_liability_policy^012.zmp_worker_comp_employer_name^013.zmp_worker_comp_employer_address^013.zmp_worker_comp_employer_address|001.street_address1^013.zmp_worker_comp_employer_address|002.street_address2^013.zmp_worker_comp_employer_address|003.city^013.zmp_worker_comp_employer_address|004.state_province^013.zmp_worker_comp_employer_address|005.zip_postal_code^014.zmp_illness_due_to_non_work_related_accident^015.zmp_auto_related_injury^016.zmp_accident_date^017.zmp_no_fault_non_liability_insurance_name^018.zmp_no_fault_non_liability_insurance_address^018.zmp_no_fault_non_liability_insurance_address|001.street_address1^018.zmp_no_fault_non_liability_insurance_address|002.street_address2^018.zmp_no_fault_non_liability_insurance_address|003.city^018.zmp_no_fault_non_liability_insurance_address|004.state_province^018.zmp_no_fault_non_liability_insurance_address|005.zip_postal_code^019.zmp_no_fault_non_liability_insurance_claim_num^020.zmp_third_party_liable^021.zmp_liability_insurer_name^022.zmp_liability_insurer_address^022.zmp_liability_insurer_address|001.street_address1^022.zmp_liability_insurer_address|002.street_address2^022.zmp_liability_insurer_address|003.city^022.zmp_liability_insurer_address|004.state_province^022.zmp_liability_insurer_address|005.zip_postal_code^023.zmp_liability_claim_num^024.zmp_medicare_eligibility_reason^025.zmp_patient_employment_status^026.zmp_patient_retirement_date^027.zmp_patient_employer_name^028.zmp_patient_employer_address^028.zmp_patient_employer_address|001.street_address1^028.zmp_patient_employer_address|002.street_address2^028.zmp_patient_employer_address|003.city^028.zmp_patient_employer_address|004.state_province^028.zmp_patient_employer_address|005.zip_postal_code^029.zmp_spouse_information^030.zmp_spouse_employment_status^031.zmp_spouse_retirement_date^032.zmp_other_employer_name^033.zmp_other_employer_address^033.zmp_other_employer_address|001.street_address1^033.zmp_other_employer_address|002.street_address2^033.zmp_other_employer_address|003.city^033.zmp_other_employer_address|004.state_province^033.zmp_other_employer_address|005.zip_postal_code^034.zmp_group_hlth_coverage^035.zmp_group_hlth_coverage_employer_size^036.zmp_group_hlth_plan^037.zmp_group_hlth_plan_address^037.zmp_group_hlth_plan_address|001.street_address1^037.zmp_group_hlth_plan_address|002.street_address2^037.zmp_group_hlth_plan_address|003.city^037.zmp_group_hlth_plan_address|004.state_province^037.zmp_group_hlth_plan_address|005.zip_postal_code^038.zmp_group_hlth_plan_member_num^039.zmp_group_hlth_plan_group_num^040.zmp_group_hlth_plan_policy_hldr_name^041.zmp_group_hlth_plan_policy_hldr_relt_to_pat^042.zmp_kidney_transplant_status^043.zmp_transplant_date^045.zmp_dialysis_start_date^046.zmp_self_dialysis_training_program_start_date^047.zmp_month_coordination_period^049.zmp_initial_entitlement_based_on_esrd^052.zmp_information_supplied_by^053.zmp_mspq_completion_status^054.zmp_mspq_completion_user^055.zmp_mspq_completion_date^056.zmp_is_eghp_self_based^057.zmp_end_stage_renal_disease^058.zmp_non_esrd_disability^059.zmp_receiving_training_for_home_dialysis^060.zmp_entitled_based_on_age^061.zmp_working_aged_or_disability_provision_applies^062.zmp_received_maintenance_dialysis_treatment^063.zmp_spouse_eghp_covrage_has_20_employees^064.zmp_sponsored_by_patient_current_employer^065.zmp_sponsored_by_spouse_current_employer^066.zmp_sponsored_by_family_member_current_employer\n' \
                'ADT,ZDT,001.zdt_set_id^002.zdt_event_type_code^003.zdt_patient_class^004.zdt_assigned_patient_location^004.zdt_assigned_patient_location|001.point_of_care^004.zdt_assigned_patient_location|002.room^004.zdt_assigned_patient_location|003.bed^004.zdt_assigned_patient_location|004.facility^004.zdt_assigned_patient_location|005.status^004.zdt_assigned_patient_location|006.person_loc_type^004.zdt_assigned_patient_location|007.building^004.zdt_assigned_patient_location|008.floor^004.zdt_assigned_patient_location|009.desc^005.zdt_event_start_date_time^006.zdt_accomodation_code^007.zdt_hospital_service^008.zdt_admission_level_care_code\n' \
                'ADT,ZIF,001.zif_patient_encounter_infection_type1^001.zif_patient_encounter_infection_type1|001.type^001.zif_patient_encounter_infection_type1|002.instant_added^001.zif_patient_encounter_infection_type1|003.instant_resolved^002.zif_patient_encounter_infection_type2^002.zif_patient_encounter_infection_type2|001.type^002.zif_patient_encounter_infection_type2|002.instant_added^002.zif_patient_encounter_infection_type2|003.instant_resolved\n' \
                'ADT,ZPV,001.zpv_clinic_code^002.zpv_client_code^003.zpv_current_flowsheet_value^003.zpv_current_flowsheet_value|001.id^003.zpv_current_flowsheet_value|002.value^004.zpv_adt_filter_flag^005.zpv_bed_phone_num^006.zpv_last_menstrual_period^007.zpv_insurance_type^008.zpv_research_study_id^009.zpv_billing_indicator^009.zpv_billing_indicator|001.n_a^009.zpv_billing_indicator|002.title^010.zpv_stop_bill_indicator^010.zpv_stop_bill_indicator|001.n_a^010.zpv_stop_bill_indicator|002.title^011.zpv_first_inpatient_occurence_date_time^012.zpv_first_outpatient_occurence_date_time^013.zpv_first_ed_patient_occurence_date_time^014.zpv_rtls_badge_id^015.zpv_ed_disposition^016.zpv_bed_type^016.zpv_bed_type|001.code^016.zpv_bed_type|002.text^017.zpv_bed_type_reason^017.zpv_bed_type_reason|001.code^017.zpv_bed_type_reason|002.text^018.zpv_room_type^018.zpv_room_type|001.code^018.zpv_room_type|002.text^019.zpv_room_type_reason^019.zpv_room_type_reason|001.code^019.zpv_room_type_reason|002.text^020.zpv_accomodation_code_reason^020.zpv_accomodation_code_reason|001.code^020.zpv_accomodation_code_reason|002.text\n' \
                'ADT,ZDU,001.zdu_set_id^002.zdu_academic_degree^003.zdu_academic_degree_program_date_range^004.zdu_academic_degree_program_part_date_range^005.zdu_academic_degree_granted_date^006.zdu_school_name^007.zdu_school_type_code^008.zdu_school_address\n' \
                'ADT,ZPD,001.zpd_patient_ancestry^002.zpd_preferred_communication_method^003.zpd_multiple_birth_total^004.zpd_my_chart_status^005.zpd_research_study_id^006.zpd_authorized_service_area^007.zpd_pregnancy_flag^008.zpd_expected_delivery_date^009.zpd_transplant_patient_flag\n' \
                'ADT,ZVN,001.zvn_triggering_workstation^001.zvn_triggering_workstation|001.id^001.zvn_triggering_workstation|002.name^001.zvn_triggering_workstation|003.n_a^001.zvn_triggering_workstation|004.dept_name^001.zvn_triggering_workstation|005.user_login_dept\n' \
                'MDM,ZUR,001.zur_set_id ^ 002.zur_information\n' \
                'ORU,ZER,001.zer_expected_arrival_date_time^002.zer_arrival_date_time^003.zer_triage_date_time^004.zer_mpi_search^005.zer_complaint^005.zer_complaint|001.complaint_code^005.zer_complaint|002.complaint_desc^006.zer_priority^006.zer_priority|001.priority_code^006.zer_priority|002.priority_name^006.zer_priority|003.priority_rank^007.zer_location^007.zer_location|001.location_code^007.zer_location|002.location_name^008.zer_room_treatment^008.zer_room_treatment|001.room_treatment_code^008.zer_room_treatment|002.room_treatment_name^009.zer_wait_list^009.zer_wait_list|001.wait_list_code^009.zer_wait_list|002.wait_list_description^009.zer_wait_list|003.wait_list_number^010.zer_current_status_event^010.zer_current_status_event|001.event_code^010.zer_current_status_event|002.event_desc^011.zer_nurse_id^012.zer_practitioner_id^013.zer_departure_information^013.zer_departure_information|001.departure_date^013.zer_departure_information|002.disposition^013.zer_departure_information|002.disposition|001.code^013.zer_departure_information|002.disposition|002.description^013.zer_departure_information|003.condition^013.zer_departure_information|003.condition|001.code^013.zer_departure_information|003.condition|002.description^013.zer_departure_information|004.departure_comment^013.zer_departure_information|005.impression_urn^014.zer_ambulance_company^014.zer_ambulance_company|001.ambulance_company^014.zer_ambulance_company|001.ambulance_company|001.mnem^014.zer_ambulance_company|001.ambulance_company|002.name^014.zer_ambulance_company|002.ambulance_company_id^014.zer_ambulance_company|003.call_date_time^015.zer_estimated_time_of_arrival^016.zer_reserved_room_treatment^016.zer_reserved_room_treatment|001.room_treatment_code^016.zer_reserved_room_treatment|002.room_treatment_name^017.zer_assessment^017.zer_assessment|001.assessment^017.zer_assessment|001.assessment|001.code^017.zer_assessment|001.assessment|002.description^017.zer_assessment|002.assessment_date_time\n' \
                'ORU,ZCL,001.zcl_counter^002.zcl_call_type^002.zcl_call_type|001.type_code^002.zcl_call_type|002.type_desc^003.zcl_caller_name^004.zcl_contact^004.zcl_contact|001.person_id^004.zcl_contact|002.type_code^005.zcl_filed_information^005.zcl_filed_information|001.file_date_time^005.zcl_filed_information|002.person_id^005.zcl_filed_information|003.type_code\n' \
                'ORU,ZSI,001.zsi_set_id^002.zsi_indicator_id^003.zsi_indicator_information\n' \
                'ORU,ZDS,001.zds_study_instance^001.zds_study_instance|001.reference_pointer^001.zds_study_instance|002.application_id^001.zds_study_instance|003.data_type^001.zds_study_instance|004.subtype\n'

    connection = pypyodbc.connect('Driver={SQL Server};'
                                  'Server=XRDCWDDBSTER01.hcadev.corpaddev.net;',
                                  'Database=HCAHl7Utils;'
                                  'Trusted_Connection=Yes;')

    cursor = connection.cursor()

    sqlcommand = (
        'SELECT Segment_ID,Field_Name,Component_Name,Sub_Component_Name,HL7_Segment_Name '
        'FROM HcaHl7Utils.dbo.HL7_Standard_Msg '
        'Order by HL7_Segment_Name, Field_Seq, Component_Seq, Sub_Component_Seq; ')

    cursor.execute(sqlcommand)
    current_segment_name = ''
    resultset = cursor.fetchall()
    cursor.close()

    f = open(create_table_prod_name, 'w')

    """
    Create table creation statements
    """
    for row in resultset:
        field_name = str(row[1]).strip()
        component_name = str(row[2]).strip()
        sub_component_name = str(row[3]).strip()
        hl7_segment_name = str(row[4]).strip()
        if hl7_segment_name not in chosen_segments:
            continue
        else:
            if current_segment_name != hl7_segment_name:
                if current_segment_name != '':
                    f.write(hl7_table_suffix('hl7.db'))
                    f.write(hl7_view_query(current_segment_name))
                f.write(hl7_table_prefix(hl7_segment_name.lower()))
                f.write(common_columns)
                current_segment_name = hl7_segment_name
            new_string = create_table_names(field_name, component_name, sub_component_name)
            f.write(new_string)
    f.write(hl7_table_suffix('hl7.db'))
    f.write(hl7_view_query(current_segment_name))
    f.close()

    final_string = ''
    current_segment_name = ''
    cur_message_type = ''
    f = open(message_type_segments_name, 'w')

    """
    Create MessageTypeSegments.csv
    """
    for row in resultset:
        current_segment = str(row[0]).strip()
        field_name = str(row[1]).strip()
        component_name = str(row[2]).strip()
        sub_component_name = str(row[3]).strip()
        hl7_segment_name = str(row[4]).strip()
        if hl7_segment_name not in chosen_segments:
            continue
        else:
            if current_segment_name != hl7_segment_name:
                if current_segment_name != '':
                    for message_type in message_type_segments.get(current_segment_name):
                        cur_message_type = message_type
                        f.write(cur_message_type + ',' + final_string + '\n')
                else:
                    f.write(message_type_header)
                current_segment_name = hl7_segment_name
                print current_segment_name
                final_string = ''
            new_string = construct_parsing_format(current_segment, field_name, component_name, sub_component_name)
            if final_string == '':
                final_string = '{0},{1}'.format(current_segment_name, new_string)
            else:
                final_string = '{0}^{1}'.format(final_string, new_string)
    f.write(cur_message_type + ',' + final_string + '\n')
    f.write(zsegments)
    f.close()
    connection.close()


if __name__ == "__main__": main()
