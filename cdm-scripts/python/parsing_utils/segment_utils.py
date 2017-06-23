import csv
import os


class SegmentUtils:
    """
    Utility methods for creating segments.txt
    It reads the template files -
    'hl7MapStandard2.8.2.csv', 'hl7MapEcw2.3.1.csv', 'hl7MapEpic2.3.csv', 'hl7MapIpeople2.5.csv',
    'hl7MapMEDITECH2.1.csv', 'hl7MapMEDITECH2.4.csv', 'hl7MapNextGen2.3.csv'
    and then merges and converts them into the segments.txt format.
    segments.txt is written to the current working directory
    """

    def __init__(self):
        pass

    default_msg_type_apply = 'ALL'

    adhoc_elements = 'MDM,ADHOC:CDI:DELIMITED:HL7CDMPATIDLOAD:::CDI,message_control_id^sending_facility^date_time_of_message^unique_document_num^unique_document_num|entity_id^unique_document_num|namespace_id^origination_date_time^transcription_date_time^message_type|message_code^message_type|trigger_event^patient_account_num^medical_record_num^patient_id_list^patient_id_list|id_num^medical_record_urn^alternate_patient_id^alternate_patient_id|id_num^date_time_of_birth^patient_name|family_name^patient_name|given_name^administrative_sex^admit_date_time^discharge_date_time^originator_code_name|person_id^originator_code_name|family_name^originator_code_name|given_name^originator_code_name|second_and_further_given_names_or_initials_thereof^originator_code_name|suffix^originator_code_name|prefix^originator_code_name|degree^filler_order_num^document_type|id^document_type|text^document_type|name_of_coding_system^document_completion_status^obsv_value\n' \
                     'ORU,ADHOC:CDI:DELIMITED:HL7CDMPATIDLOAD:::CDI,message_control_id^sending_facility^date_time_of_message^filler_order_num^placer_order_num^specimen_action_code^requested_date_time^obsv_date_time_num^message_type|message_code^message_type|trigger_event^patient_account_num^medical_record_num^patient_id_list^patient_id_list|id_num^medical_record_urn^alternate_patient_id^alternate_patient_id|id_num^date_time_of_birth^patient_name|family_name^patient_name|given_name^administrative_sex^admit_date_time^discharge_date_time^ordering_provider|person_id^ordering_provider|family_name^ordering_provider|given_name^ordering_provider|second_and_further_given_names_or_initials_thereof^ordering_provider|suffix^ordering_provider|prefix^ordering_provider|degree^result_status^universal_service_id|id^universal_service_id|text^universal_service_id|name_of_coding_system^obsv_result_status^obsv_value\n' \
                     'MDM,ADHOC:SCRIPATID:JSON&scri_mdm_key_names.csv:HL7CDMSCRIPATID_JSON:scri_mdm_filters.csv:document_completion_status:DR,message_control_id^sending_facility^origination_date_time^transcription_date_time^message_type|message_code^message_type|trigger_event^patient_account_num^medical_record_num^medical_record_urn^date_time_of_birth^patient_name|family_name^patient_name|given_name^administrative_sex^admit_date_time^discharge_date_time^originator_code_name|person_id^originator_code_name|family_name^originator_code_name|given_name^originator_code_name|second_and_further_given_names_or_initials_thereof^originator_code_name|suffix^originator_code_name|prefix^originator_code_name|degree^filler_order_num^document_type|id^document_type|text^document_type|name_of_coding_system^document_completion_status^obsv_value\n' \
                     'ORU,ADHOC:SCRIPATID:JSON&scri_oru_key_names.csv:HL7CDMSCRIPATID_JSON:scri_oru_filters.csv:obsv_result_status:DR,message_control_id^sending_facility^requested_date_time^obsv_date_time_num^message_type|message_code^message_type|trigger_event^patient_account_num^medical_record_num^medical_record_urn^date_time_of_birth^patient_name|family_name^patient_name|given_name^administrative_sex^admit_date_time^discharge_date_time^ordering_provider|person_id^ordering_provider|family_name^ordering_provider|given_name^ordering_provider|second_and_further_given_names_or_initials_thereof^ordering_provider|suffix^ordering_provider|prefix^ordering_provider|degree^result_status^universal_service_id|id^universal_service_id|text^universal_service_id|name_of_coding_system^obsv_result_status^obsv_value\n' \
                     'MDM,ADHOC:SCRIPATIDHIST:DELIMITED:HL7CDMSCRIPATIDJSONHIST:scri_mdm_filters.csv:document_completion_status,message_control_id^sending_facility^medical_record_num^medical_record_urn^patient_account_num^origination_date_time^transcription_date_time^message_type|message_code^message_type|trigger_event^date_time_of_birth^patient_name|family_name^patient_name|given_name^administrative_sex^admit_date_time^discharge_date_time^originator_code_name|person_id^originator_code_name|family_name^originator_code_name|given_name^originator_code_name|second_and_further_given_names_or_initials_thereof^originator_code_name|suffix^originator_code_name|prefix^originator_code_name|degree^filler_order_num^document_type|id^document_type|text^document_type|name_of_coding_system^document_completion_status^obsv_value^ssn_num_patient^patient_address|street_address^patient_address|other_designation^patient_address|city^patient_address|state_or_province^patient_address|zip_or_postal_code\n' \
                     'ORU,ADHOC:SCRIPATIDHIST:DELIMITED:HL7CDMSCRIPATIDJSONHIST:scri_oru_filters.csv:obsv_result_status,message_control_id^sending_facility^medical_record_num^medical_record_urn^patient_account_num^requested_date_time^obsv_date_time_num^message_type|message_code^message_type|trigger_event^date_time_of_birth^patient_name|family_name^patient_name|given_name^administrative_sex^admit_date_time^discharge_date_time^ordering_provider|person_id^ordering_provider|family_name^ordering_provider|given_name^ordering_provider|second_and_further_given_names_or_initials_thereof^ordering_provider|suffix^ordering_provider|prefix^ordering_provider|degree^result_status^universal_service_id|id^universal_service_id|text^universal_service_id|name_of_coding_system^obsv_result_status^obsv_value^ssn_num_patient^patient_address|street_address^patient_address|other_designation^patient_address|city^patient_address|state_or_province^patient_address|zip_or_postal_code\n' \
                     'ADT,ADHOC:SCRIINAVIGATE:JSON&scri_inagivate_key_names.csv:HL7CDMSCRIINAVIGATEADT:scri_inagivate_filters.csv,message_control_id^patient_account_num^patient_address|street_address^patient_address|other_designation^patient_address|city^patient_address|country^patient_address|county_parish_code|text^date_time_of_birth^patient_e_mail_address^phone_num_home|communication_address^phone_num_home|telecommunication_use_code^sending_facility^sending_application^patient_name|given_name^insurance_company_name^primary_language^patient_name|family_name^patient_name|second_and_further_given_names_or_initials_thereof^medical_record_num^address|street_address^address|other_designation^business_phone_num^address|city^address|country^address|zip_or_postal_code^name^phone_num^relationship^address|state_or_province^address|zip_or_postal_code^phone_num_home^race^date_time_of_message^administrative_sex^ssn_num_patient^patient_address|state_or_province^medical_record_urn^patient_address|zip_or_postal_code\n' \
                     'RAS,ADHOC:EFMEDADMIN:JSON&east_florida_medadmin_key_names.csv:EFMEDADMINRAS:east_florida_medadmin_filters.csv,message_control_id^sending_facility^patient_account_num^medical_record_num^medical_record_urn^placer_order_num^filler_order_num^administration_sub_id_counter^patient_type^date_time_end_of_administration^date_time_start_of_administration^administration_site^administered_dosage_form|id^administered_units^indication|text^indication|id^administering_provider\n'

    file_names = ['hl7MapStandard2.8.2.csv', 'hl7MapEcw2.3.1.csv', 'hl7MapEpic2.3.csv', 'hl7MapIpeople2.5.csv',
                  'hl7MapMT6_2.4.csv', 'hl7MapMT6_2.5.1.csv', 'hl7MapMT6_2.5.csv', 'hl7MapMT_2.1.csv',
                  'hl7MapMT_2.2.csv', 'hl7MapMT_2.4.csv', 'hl7MapMT_2.5.csv', 'hl7MapNextGen2.3.csv']

    underscore_dict = {'MSA', 'NTE', 'PSL', 'RF1', 'SAC', 'ZER', 'AUT', 'FT1', 'ZRX', 'ZIV', 'ZPE', 'ZST'}

    skip_dict = {'MSH:field_separator'}

    @staticmethod
    def get_parent_directory(directory):
        """
        Get the parent directory for a given directory
        :param directory: directory to find the parent of
        :return: parent directory
        """
        return os.path.dirname(directory)

    @staticmethod
    def get_templates_dir():
        """
        Get the template directory
        :return: location of the templates used for creating segments
        """
        cwd = os.getcwd()
        base_path = SegmentUtils.get_parent_directory(SegmentUtils.get_parent_directory(SegmentUtils.get_parent_directory(cwd)))
        return base_path + '\\cdm-hl7-parser\\src\\main\\resources\\templates\\'

    @staticmethod
    def construct_parsing_format(field1, field2, field3):
        """
        Creates the MessageTypeSegment parsing format
        :param field1: segment
        :param field2: component
        :param field3: subcomponent
        :return: formatted segment, component, subcomponent
        """
        if field2 == 'None' or field2 == '':
            return '{0}'.format(field1)
        elif field3 == 'None' or field3 == '':
            return '{0}|{1}'.format(field1, field2)
        else:
            return '{0}|{1}|{2}'.format(field1, field2, field3)

    @staticmethod
    def unicode_csv_reader(utf8_data, dialect=csv.excel, **kwargs):
        """
        Reads a csv file and removes any strange characters
        :param utf8_data: file to read
        :param dialect: specific dialect for parsing
        :param kwargs: extra arguments
        :returns: unicode encoded rows from csv file
        """
        csv_reader = csv.reader(utf8_data, dialect=dialect, **kwargs)
        for row in csv_reader:
            yield [unicode(cell, 'utf-8-sig') for cell in row]

    @staticmethod
    def add_prefix_underscore(string, dup_dict):
        if string in dup_dict:
            return '_' + string
        else:
            return string

