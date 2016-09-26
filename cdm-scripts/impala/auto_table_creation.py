import pypyodbc


def add_indexes(field):
    if len(field) > 1:
        return '{0}'.format('0' + field + '.')
    else:
        return '{0}'.format('00' + field + '.')


def construct_parsing_field(segment, field):
    return add_indexes(segment) + field


def construct_parsing_format(current_seg, field1, field2, field3):
    splitted_seg = current_seg.split('.')
    if field2 == 'None':
        return '{0}'.format(construct_parsing_field(splitted_seg[1], field1))
    elif field3 == 'None':
        return '{0}|{1}'.format(construct_parsing_field(splitted_seg[1], field1), construct_parsing_field(splitted_seg[2], field2))
    else:
        return '{0}|{1}|{2}'.format(construct_parsing_field(splitted_seg[1], field1),
                                    construct_parsing_field(splitted_seg[2], field2),
                                    construct_parsing_field(splitted_seg[3], field3))


def create_table_names(field1, field2, field3):
    if field2 == 'None':
        return '{0} String,'.format(field1)
    elif field3 == 'None':
        return '{0}_{1} String,'.format(field1, field2)
    else:
        return '{0}_{1}_{2} String,'.format(field1, field2, field3)


def hl7_table_prefix(segment):
    return 'CREATE EXTERNAL TABLE hl7_{0}_data ('.format(segment)


def hl7_table_suffix(db):
    return 'etl_firstinsert_datetime String' \
           '\n) ' \
           '\nROW FORMAT DELIMITED' \
           '\nFIELDS TERMINATED BY \'|\'' \
           '\nSTORED AS SEQUENCEFILE' \
           '\nLOCATION \'/user/hive/warehouse/{0}/hl7_all_segment_data\';\n'.format(db)


def hl7_view_query(segment_lower, segment_upper):
    return 'CREATE VIEW vw_hl7_{0}_data AS SELECT * FROM hl7_{0}_data WHERE segment_type=\'{1}\';\n'.format(
        segment_lower,
        segment_upper)


common_columns = 'segment_type String,\nmsh_sending_facility String,\nmsh_msg_control_id String,' \
                 '\npid_medical_record_num String,\npid_medical_record_urn String,\npid_pat_account_num String,'

connection = pypyodbc.connect('Driver={SQL Server};'
                              'Server=XRDCWDDBSTER01.hcadev.corpaddev.net;'
                              'Database=HCAHl7Utils;'
                              'Trusted_Connection=Yes;')

cursor = connection.cursor()

SQLCommand = (
    'SELECT Segment_ID,Field_Name,Component_Name,Sub_Component_Name,HL7_Segment_Name '
    'FROM HcaHl7Utils.dbo.HL7_Standard_Msg '
    'Order by HL7_Segment_Name, Field_Seq, Component_Seq, Sub_Component_Seq; ')

cursor.execute(SQLCommand)

current_segment_name = ''
resultset = cursor.fetchall()
for row in resultset:
    current_segment = str(row[0]).strip()
    field_name = str(row[1]).strip()
    component_name = str(row[2]).strip()
    sub_component_name = str(row[3]).strip()
    hl7_segment_name = str(row[4]).strip()
    if current_segment_name != hl7_segment_name:
        if (current_segment_name != ''):
            print hl7_table_suffix('hl7.db')
            print hl7_view_query(current_segment_name.lower(), current_segment_name.upper())
        print hl7_table_prefix(hl7_segment_name.lower())
        print common_columns
        current_segment_name = hl7_segment_name
    new_string = create_table_names(field_name, component_name, sub_component_name)
    print new_string
print hl7_table_suffix('hl7.db')
print hl7_view_query(current_segment_name.lower(), current_segment_name.upper())

final_string = ''
current_segment_name = ''
for row in resultset:
    current_segment = str(row[0]).strip()
    field_name = str(row[1]).strip()
    component_name = str(row[2]).strip()
    sub_component_name = str(row[3]).strip()
    hl7_segment_name = str(row[4]).strip()
    if current_segment_name != hl7_segment_name:
        print final_string
        final_string = ''
        current_segment_name = hl7_segment_name
    new_string = construct_parsing_format(current_segment, field_name, component_name, sub_component_name)
    if final_string == '':
        final_string = ',{0},{1}'.format(current_segment_name, new_string)
    else:
        final_string = '{0}^{1}'.format(final_string, new_string)

connection.close()