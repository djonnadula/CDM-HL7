import csv
import yaml


def read_props():
    """
    Read yaml properties file
    """
    yaml_props = {}
    with open("create_tables_props.yml", 'r') as stream:
        try:
            yaml_props = yaml.load(stream)
        except yaml.YAMLError as exc:
            print (exc)
    return yaml_props


def hl7_drop_table(segment):
    """
    Drop table template
    :param segment: hl7 segment name
    :return: drop table statement string
    """
    return '\nDROP TABLE hl7_{0}_data;\n'.format(segment.lower())


def hl7_table_prefix(segment, environment):
    """
    Prefix for all create table statements
    :param segment: hl7 segment name
    :return: create table prefix statement string
    """
    return '\nCREATE EXTERNAL TABLE {1}.hl7_{0} ('.format(segment.lower(), environment)


def hl7_table_suffix(db_path, message_type):
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
           '\nLOCATION \'/user/hive/warehouse/{0}/landing_zone=SEGMENTS/hl7_segment={1}\';\n'.format(db_path.lower(),
                                                                                                         message_type)


def use_query():
    """
    Use query
    """
    return 'USE hl7;\n'


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


def clean_comps(comps):
    """
    Create the table column name format
    :param comps: array of components to join
    :return: formatted column name
    """
    joiner = '_'
    column_name = ''
    if len(comps) < 2:
        column_name = comps[0]
    elif len(comps) >= 2:
        column_name = joiner.join(comps)
    res = '\t{0} STRING,\n'.format(column_name)
    return res


def common_columns():
    """
    String of common columns that all tables have
    """
    return '\n\tsending_facility STRING,' \
           '\n\tmessage_control_id STRING,' \
           '\n\tmedical_record_num STRING,' \
           '\n\tmedical_record_urn STRING,' \
           '\n\tpatient_account_num STRING,\n'


def write_tables(env, add_drop_tables, db_name, db_path):
    """
    Create the table column name format
    :param env: environment for file name
    :param add_drop_tables: boolean that defines whether write drop tables or not
    :param db_name: database name from yaml properties
    :param db_path: database location path from yaml properties
    """
    f = open('create_impala_tables_{0}.sql'.format(env), 'w')
    with open('..\\..\\templates\\segments.txt', 'rU') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            all = row[0]
            segment_name = row[1]
            components = row[2]
            if all != 'ALL':
                continue
            else:
                # print segment_name
                if add_drop_tables:
                    f.write(hl7_drop_table(segment_name))
                f.write(hl7_table_prefix(segment_name, db_name))
                f.write(common_columns())
                for element in components.split('^'):
                    comps = element.split('|')
                    cleaned_comps = clean_comps(comps)
                    f.write(cleaned_comps)
                f.write(hl7_table_suffix(db_path, segment_name))
    f.close()


def main():
    """
    Main program logic
    """
    yaml_props = read_props()
    # print yaml_props
    environment = yaml_props.get('environment')
    environments = yaml_props.get('environments')
    add_drop_tables = yaml_props.get('add_drop_tables')

    for env in environment:
        db_name = environments.get(env).get('db_name')
        db_path = environments.get(env).get('db_path')
        write_tables(env, add_drop_tables, db_name, db_path)

if __name__ == "__main__": main()