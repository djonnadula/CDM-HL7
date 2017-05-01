import csv


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
    csv_reader = csv.reader(utf8_data, dialect=dialect, **kwargs)
    for row in csv_reader:
        yield [unicode(cell, 'utf-8-sig') for cell in row]


def main():
    message_type_header = 'MDM,ADHOC:CDI:DELIMITED:HL7CDMPATIDLOAD:::CDI,msg_control_id^sending_facility^msg_create_date_time^unique_document_num^unique_document_num|entity_id^unique_document_num|namespace_id^origination_date_time^transcription_date_time^msg_type|message_code^msg_type|trigger_event^pat_account_num^medical_record_num^pat_id_list^pat_id_list|identifier_num^medical_record_urn^alt_pat_id^alt_pat_id|identifier_num^birth_date_time^pat_name|last_name^pat_name|first_name^sex^adm_date_time^dchg_date_time^originator_code_name|person_id^originator_code_name|last_name^originator_code_name|first_name^originator_code_name|middle_name^originator_code_name|suffix^originator_code_name|prefix^originator_code_name|degree^filler_order_num^document_type|primary_id^document_type|text^document_type|name_of_coding_sys^document_completion_status^observation_value\n' \
                          'ORU,ADHOC:CDI:DELIMITED:HL7CDMPATIDLOAD:::CDI,msg_control_id^sending_facility^msg_create_date_time^filler_order_num^placer_order_num^specimen_action_code^requested_date_time^observation_date_time^msg_type|message_code^msg_type|trigger_event^pat_account_num^medical_record_num^pat_id_list^pat_id_list|identifier_num^medical_record_urn^alt_pat_id^alt_pat_id|identifier_num^birth_date_time^pat_name|last_name^pat_name|first_name^sex^adm_date_time^dchg_date_time^ordering_provider|person_id^ordering_provider|last_name^ordering_provider|first_name^ordering_provider|middle_name^ordering_provider|suffix^ordering_provider|prefix^ordering_provider|degree^result_status^universal_service_id|identifier^universal_service_id|text^universal_service_id|name_of_coding_sys^observation_result_status^observation_value\n' \
                          'MDM,ADHOC:SCRIPATID:JSON&scri_mdm_key_names.csv:HL7CDMSCRIPATID_JSON:scri_mdm_filters.csv:document_completion_status:DR,msg_control_id^sending_facility^origination_date_time^transcription_date_time^msg_type|message_code^msg_type|trigger_event^pat_account_num^medical_record_num^medical_record_urn^birth_date_time^pat_name|last_name^pat_name|first_name^sex^adm_date_time^dchg_date_time^originator_code_name|person_id^originator_code_name|last_name^originator_code_name|first_name^originator_code_name|middle_name^originator_code_name|suffix^originator_code_name|prefix^originator_code_name|degree^filler_order_num^document_type|primary_id^document_type|text^document_type|name_of_coding_sys^document_completion_status^observation_value\n' \
                          'ORU,ADHOC:SCRIPATID:JSON&scri_oru_key_names.csv:HL7CDMSCRIPATID_JSON:scri_oru_filters.csv:observation_result_status:DR,msg_control_id^sending_facility^requested_date_time^observation_date_time^msg_type|message_code^msg_type|trigger_event^pat_account_num^medical_record_num^medical_record_urn^birth_date_time^pat_name|last_name^pat_name|first_name^sex^adm_date_time^dchg_date_time^ordering_provider|person_id^ordering_provider|last_name^ordering_provider|first_name^ordering_provider|middle_name^ordering_provider|suffix^ordering_provider|prefix^ordering_provider|degree^result_status^universal_service_id|identifier^universal_service_id|text^universal_service_id|name_of_coding_sys^observation_result_status^observation_value\n' \
                          'MDM,ADHOC:SCRIPATIDHIST:DELIMITED:HL7CDMSCRIPATIDJSONHIST:scri_mdm_filters.csv:document_completion_status,msg_control_id^sending_facility^medical_record_num^medical_record_urn^pat_account_num^origination_date_time^transcription_date_time^msg_type|message_code^msg_type|trigger_event^birth_date_time^pat_name|last_name^pat_name|first_name^sex^adm_date_time^dchg_date_time^originator_code_name|person_id^originator_code_name|last_name^originator_code_name|first_name^originator_code_name|middle_name^originator_code_name|suffix^originator_code_name|prefix^originator_code_name|degree^filler_order_num^document_type|primary_id^document_type|text^document_type|name_of_coding_sys^document_completion_status^observation_value^pat_social_security_num^pat_address|street_address1^pat_address|street_address2^pat_address|city^pat_address|state_province^pat_address|zip_postal_code\n' \
                          'ORU,ADHOC:SCRIPATIDHIST:DELIMITED:HL7CDMSCRIPATIDJSONHIST:scri_oru_filters.csv:observation_result_status,msg_control_id^sending_facility^medical_record_num^medical_record_urn^pat_account_num^requested_date_time^observation_date_time^msg_type|message_code^msg_type|trigger_event^birth_date_time^pat_name|last_name^pat_name|first_name^sex^adm_date_time^dchg_date_time^ordering_provider|person_id^ordering_provider|last_name^ordering_provider|first_name^ordering_provider|middle_name^ordering_provider|suffix^ordering_provider|prefix^ordering_provider|degree^result_status^universal_service_id|identifier^universal_service_id|text^universal_service_id|name_of_coding_sys^observation_result_status^observation_value^pat_social_security_num^pat_address|street_address1^pat_address|street_address2^pat_address|city^pat_address|state_province^pat_address|zip_postal_code\n' \
                          'ADT,ADHOC:SCRIINAVIGATE:JSON&scri_inagivate_key_names.csv:HL7CDMSCRIINAVIGATEADT:scri_inagivate_filters.csv,msg_control_id^pat_account_num^pat_address|street_address1^pat_address|street_address2^pat_address|city^pat_address|country^pat_address|county_parish_code|text^birth_date_time^patient_e_mail_address^home_phone_num|comm_address^home_phone_num|use_code^sending_facility^sending_appl^pat_name|first_name^insurance_companay_name^primary_language^pat_name|last_name^pat_name|middle_name^medical_record_num^address|street_address1^address|street_address2^business_phone_num^address|city^address|country^address|zip_postal_code^name^phone_num^relationship^address|state_province^address|zip_postal_code^home_phone_num^race^msg_create_date_time^sex^pat_social_security_num^pat_address|state_province^medical_record_urn^pat_address|zip_postal_code\n'

    cur_message_type = 'ALL'
    final_string = ''
    f = open('segments.txt', 'w')
    current_segment = ''
    templates_dict = {}
    listy = []
    file_names = ['hl7MapStandard2.8.2.csv', 'hl7MapEcw2.3.1.csv', 'hl7MapEpic2.3.csv', 'hl7MapIpeople2.5.csv',
                 'hl7MapMEDITECH2.1.csv', 'hl7MapMEDITECH2.4.csv', 'hl7MapNextGen2.3.csv']

    for filename in file_names:
        with open('..\\templates\\' + filename, 'rU') as csvFile:
            reader = unicode_csv_reader(csvFile, delimiter=',')
            resList = list(reader)
            if filename == 'hl7MapStandard2.8.2.csv':
                templates_dict['1' + filename] = resList
            else:
                templates_dict['2' + filename] = resList

    for row in templates_dict:
        val = templates_dict.get(row)
        for r in val:
            # r[1] = r[1].strip()[4:]
            index = r[0].strip()
            field = r[1].strip()
            segment_name = index.split('.')[0]
            component = r[2].strip()
            subcomponent = r[3].strip()
            o = (row, segment_name, index, field, component, subcomponent)
            listy.append(o)

    sorted_listy = sorted(listy, key=lambda x: (x[1], x[0]))

    for index in sorted_listy:
        segment_name = index[1]
        field = index[3]
        component = index[4]
        subcomponent = index[5]
        if current_segment != segment_name:
            if current_segment != '':
                f.write(cur_message_type + ',' + final_string + '\n')
            else:
                f.write(message_type_header)
            current_segment = segment_name
            final_string = ''
        new_string = construct_parsing_format(field, component, subcomponent)
        if final_string == '':
            final_string = '{0},unknown^{1}'.format(str(current_segment).upper(), new_string)
        else:
            final_string = '{0}^{1}'.format(final_string, new_string)

    f.write(cur_message_type + ',' + final_string + '\n')
    f.close()


if __name__ == "__main__": main()
