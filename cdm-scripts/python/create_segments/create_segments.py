import csv
import sets

"""
Script to create the segments.txt which is used for HL7 data parsing
It reads the template files -
'hl7MapStandard2.8.2.csv', 'hl7MapEcw2.3.1.csv', 'hl7MapEpic2.3.csv', 'hl7MapIpeople2.5.csv',
'hl7MapMEDITECH2.1.csv', 'hl7MapMEDITECH2.4.csv', 'hl7MapNextGen2.3.csv'
and then merges and converts them into the segments.txt format.
segments.txt is written to the current working directory
"""


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


def unicode_csv_reader(utf8_data, dialect=csv.excel, **kwargs):
    """
    Reads a csv file and removes any strange characters
    :param utf8_data: file to read
    :param dialect: specific dialet for parsing
    :param kwargs: extra arguments
    :returns: unicode encoded rows from csv file
    """
    csv_reader = csv.reader(utf8_data, dialect=dialect, **kwargs)
    for row in csv_reader:
        yield [unicode(cell, 'utf-8-sig') for cell in row]


def main():
    """
    Main program logic
    """

    # ADHOC parsing instructions
    message_type_header = 'MDM,ADHOC:CDI:DELIMITED:HL7CDMPATIDLOAD:::CDI,message_control_id^sending_facility^date_time_of_message^unique_document_num^unique_document_num|entity_id^unique_document_num|namespace_id^origination_date_time^transcription_date_time^message_type|message_code^message_type|trigger_event^patient_account_num^medical_record_num^patient_id_list^patient_id_list|id_num^medical_record_urn^alternate_patient_id^alternate_patient_id|id_num^date_time_of_birth^patient_name|family_name^patient_name|given_name^administrative_sex^admit_date_time^discharge_date_time^originator_code_name|person_id^originator_code_name|family_name^originator_code_name|given_name^originator_code_name|second_and_further_given_names_or_initials_thereof^originator_code_name|suffix^originator_code_name|prefix^originator_code_name|degree^filler_order_num^document_type|id^document_type|text^document_type|name_of_coding_system^document_completion_status^obsv_value\n' \
                          'ORU,ADHOC:CDI:DELIMITED:HL7CDMPATIDLOAD:::CDI,message_control_id^sending_facility^date_time_of_message^filler_order_num^placer_order_num^specimen_action_code^requested_date_time^obsv_date_time_num^message_type|message_code^message_type|trigger_event^patient_account_num^medical_record_num^patient_id_list^patient_id_list|id_num^medical_record_urn^alternate_patient_id^alternate_patient_id|id_num^date_time_of_birth^patient_name|family_name^patient_name|given_name^administrative_sex^admit_date_time^discharge_date_time^ordering_provider|person_id^ordering_provider|family_name^ordering_provider|given_name^ordering_provider|second_and_further_given_names_or_initials_thereof^ordering_provider|suffix^ordering_provider|prefix^ordering_provider|degree^result_status^universal_service_id|id^universal_service_id|text^universal_service_id|name_of_coding_system^obsv_result_status^obsv_value\n' \
                          'MDM,ADHOC:SCRIPATID:JSON&scri_mdm_key_names.csv:HL7CDMSCRIPATID_JSON:scri_mdm_filters.csv:document_completion_status:DR,message_control_id^sending_facility^origination_date_time^transcription_date_time^message_type|message_code^message_type|trigger_event^patient_account_num^medical_record_num^medical_record_urn^date_time_of_birth^patient_name|family_name^patient_name|given_name^administrative_sex^admit_date_time^discharge_date_time^originator_code_name|person_id^originator_code_name|family_name^originator_code_name|given_name^originator_code_name|second_and_further_given_names_or_initials_thereof^originator_code_name|suffix^originator_code_name|prefix^originator_code_name|degree^filler_order_num^document_type|id^document_type|text^document_type|name_of_coding_system^document_completion_status^obsv_value\n' \
                          'ORU,ADHOC:SCRIPATID:JSON&scri_oru_key_names.csv:HL7CDMSCRIPATID_JSON:scri_oru_filters.csv:obsv_result_status:DR,message_control_id^sending_facility^requested_date_time^obsv_date_time_num^message_type|message_code^message_type|trigger_event^patient_account_num^medical_record_num^medical_record_urn^date_time_of_birth^patient_name|family_name^patient_name|given_name^administrative_sex^admit_date_time^discharge_date_time^ordering_provider|person_id^ordering_provider|family_name^ordering_provider|given_name^ordering_provider|second_and_further_given_names_or_initials_thereof^ordering_provider|suffix^ordering_provider|prefix^ordering_provider|degree^result_status^universal_service_id|id^universal_service_id|text^universal_service_id|name_of_coding_system^obsv_result_status^obsv_value\n' \
                          'MDM,ADHOC:SCRIPATIDHIST:DELIMITED:HL7CDMSCRIPATIDJSONHIST:scri_mdm_filters.csv:document_completion_status,message_control_id^sending_facility^medical_record_num^medical_record_urn^patient_account_num^origination_date_time^transcription_date_time^message_type|message_code^message_type|trigger_event^date_time_of_birth^patient_name|family_name^patient_name|given_name^administrative_sex^admit_date_time^discharge_date_time^originator_code_name|person_id^originator_code_name|family_name^originator_code_name|given_name^originator_code_name|second_and_further_given_names_or_initials_thereof^originator_code_name|suffix^originator_code_name|prefix^originator_code_name|degree^filler_order_num^document_type|id^document_type|text^document_type|name_of_coding_system^document_completion_status^obsv_value^ssn_num_patient^patient_address|street_address^patient_address|other_designation^patient_address|city^patient_address|state_or_province^patient_address|zip_or_postal_code\n' \
                          'ORU,ADHOC:SCRIPATIDHIST:DELIMITED:HL7CDMSCRIPATIDJSONHIST:scri_oru_filters.csv:obsv_result_status,message_control_id^sending_facility^medical_record_num^medical_record_urn^patient_account_num^requested_date_time^obsv_date_time_num^message_type|message_code^message_type|trigger_event^date_time_of_birth^patient_name|family_name^patient_name|given_name^administrative_sex^admit_date_time^discharge_date_time^ordering_provider|person_id^ordering_provider|family_name^ordering_provider|given_name^ordering_provider|second_and_further_given_names_or_initials_thereof^ordering_provider|suffix^ordering_provider|prefix^ordering_provider|degree^result_status^universal_service_id|id^universal_service_id|text^universal_service_id|name_of_coding_system^obsv_result_status^obsv_value^ssn_num_patient^patient_address|street_address^patient_address|other_designation^patient_address|city^patient_address|state_or_province^patient_address|zip_or_postal_code\n' \
                          'ADT,ADHOC:SCRIINAVIGATE:JSON&scri_inagivate_key_names.csv:HL7CDMSCRIINAVIGATEADT:scri_inagivate_filters.csv,message_control_id^patient_account_num^patient_address|street_address^patient_address|other_designation^patient_address|city^patient_address|country^patient_address|county_parish_code|text^date_time_of_birth^patient_e_mail_address^phone_num_home|communication_address^phone_num_home|telecommunication_use_code^sending_facility^sending_application^patient_name|given_name^insurance_company_name^primary_language^patient_name|family_name^patient_name|second_and_further_given_names_or_initials_thereof^medical_record_num^address|street_address^address|other_designation^business_phone_num^address|city^address|country^address|zip_or_postal_code^name^phone_num^relationship^address|state_or_province^address|zip_or_postal_code^phone_num_home^race^date_time_of_message^administrative_sex^ssn_num_patient^patient_address|state_or_province^medical_record_urn^patient_address|zip_or_postal_code\n'

    cur_message_type = 'ALL'
    final_string = ''
    f = open('segments.txt', 'w')
    current_segment = ''
    templates_dict = {}
    listy = []
    # List of template files for parsing
    file_names = ['hl7MapStandard2.8.2.csv', 'hl7MapEcw2.3.1.csv', 'hl7MapEpic2.3.csv', 'hl7MapIpeople2.5.csv',
                  'hl7MapMEDITECH5.csv', 'hl7MapMEDITECH6.csv', 'hl7MapNextGen2.3.csv']

    # Add a 1 or 2 to filenames and create a dictionary so that they are easy to sort
    for filename in file_names:
        with open('..\\..\\templates\\' + filename, 'rU') as csvFile:
            reader = unicode_csv_reader(csvFile, delimiter=',')
            reslist = list(reader)
            if filename == 'hl7MapStandard2.8.2.csv':
                templates_dict['1' + filename] = reslist
            else:
                templates_dict['2' + filename] = reslist

    # Format data into new list
    for row in templates_dict:
        val = templates_dict.get(row)
        for r in val:
            # r[1] = r[1].strip()[4:]
            index = r[0].strip()
            field = r[1].strip()
            segment_name = index.split('.')[0]
            component = r[2].strip()
            sub_component = r[3].strip()
            o = (row, segment_name, index, field, component, sub_component)
            listy.append(o)

    # Sort data such that the hl7MapStandard2.8.2.csv is always first
    sorted_listy = sorted(listy, key=lambda x: (x[1], x[0]))

    final_set = set()
    # Main program logic - writes the formatted lines to segments.txt
    for index in sorted_listy:
        segment_name = index[1]
        field = index[3]
        component = index[4]
        sub_component = index[5]

        if segment_name == 'GT1':
            print segment_name, field, component, sub_component
        if current_segment != segment_name:
            if current_segment != '':
                f.write(cur_message_type + ',' + final_string + '\n')
            else:
                f.write(message_type_header)
            current_segment = segment_name
            final_string = ''
        new_string = construct_parsing_format(field, component, sub_component)
        if final_string == '':
            final_string = '{0},unknown^{1}'.format(str(current_segment).upper(), new_string)
        else:
            final_string = '{0}^{1}'.format(final_string, new_string)

    f.write(cur_message_type + ',' + final_string + '\n')
    f.close()


# Have a main because I'm a Java programmer
if __name__ == "__main__": main()
