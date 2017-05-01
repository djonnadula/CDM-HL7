import csv


def hl7_drop_table(segment):
    """
    Drop table template
    :param segment: hl7 segment name
    :return: drop table statement string
    """
    return '\nDROP TABLE hl7_{0}_data;\n'.format(segment.lower())


def hl7_table_prefix(segment):
    """
    Prefix for all create table statements
    :param segment: hl7 segment name
    :return: create table prefix statement string
    """
    return '\nCREATE EXTERNAL TABLE hl7_{0} ('.format(segment.lower())


def hl7_table_suffix(db, message_type):
    """
    The suffix for all create table statements
    :param db: hive database name
    :return: create table statement suffix string
    """
    return '\tetl_firstinsert_datetime STRING,\n' \
           '\tfield_sequence_num STRING\n' \
           ')\n' \
           'PARTITIONED BY (\n' \
           '\tmessage_type STRING,\n' \
           '\ttransaction_date STRING\n' \
           ')' \
           '\nROW FORMAT DELIMITED' \
           '\nFIELDS TERMINATED BY \'|\'' \
           '\nSTORED AS SEQUENCEFILE' \
           '\nLOCATION \'/user/hive/warehouse/{0}/dev/landing_zone=SEGMENTS/hl7_segment={1}\';\n'.format(db.lower(),
                                                                                                         message_type)


def create_table_names(field1, field2, field3):
    """
    Creates the table name based on how many subcomponents are present
    :param field1: segment
    :param field2: component
    :param field3: subcomponent
    :return: formatted table name
    """
    if field2 == 'None' or field2 == '':
        return '\t{0} STRING,\n'.format(field1)
    elif field3 == 'None' or field3 == '':
        return '\t{0}_{1} STRING,\n'.format(field1, field2)
    else:
        return '\t{0}_{1}_{2} STRING,\n'.format(field1, field2, field3)


def hl7_view_query(segment):
    """
    Create view query string creator
    :param segment: segment name
    :return: create view statement string
    """
    return '\nCREATE VIEW vw_hl7_{0}_data AS SELECT * FROM hl7_{0}_data WHERE segment_type=\'{1}\';\n'.format(
        segment.lower(), segment.upper())


def clean_comps(comps):
    joiner = '_'
    column_name = ''
    if len(comps) < 2:
        column_name = comps[0]
    elif len(comps) >= 2:
        column_name = joiner.join(comps)
    res = '\t{0} STRING,\n'.format(column_name)
    return res


def main():
    current_segment = ''
    common_columns = '\n\tsending_facility STRING,' \
                     '\n\tmsg_control_id STRING,' \
                     '\n\tmedical_record_num STRING,' \
                     '\n\tmedical_record_urn STRING,' \
                     '\n\tpat_account_num STRING,\n'
    use_hl7_query = 'USE hl7;\n'
    f = open('tables.sql', 'w')

    with open('..\\templates\\segments.txt', 'rU') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            all = row[0]
            segment_name = row[1]
            components = row[2]
            if all != 'ALL':
                continue
            else:
                print segment_name
                # f.write(hl7_drop_table(segment_name))
                f.write(hl7_table_prefix(segment_name))
                f.write(common_columns)
                for element in components.split('^'):
                    comps = element.split('|')
                    cleaned_comps = clean_comps(comps)
                    f.write(cleaned_comps)
                f.write(hl7_table_suffix('hl7.db', segment_name))
                # f.write(hl7_view_query(segment_name))
    f.close()


if __name__ == "__main__": main()
