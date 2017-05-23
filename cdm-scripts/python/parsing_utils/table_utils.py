import yaml
import datetime


class TableUtils:
    """
    Utils class for table creation logic
    """

    def __init__(self):
        pass

    common_columns = '\n\tetl_firstinsert_datetime STRING,' \
                     '\n\tfield_sequence_num STRING,' \
                     '\n\tsending_facility STRING,' \
                     '\n\tmessage_control_id STRING,' \
                     '\n\tmedical_record_num STRING,' \
                     '\n\tmedical_record_urn STRING,' \
                     '\n\tpatient_account_num STRING,\n'

    @staticmethod
    def read_props(file_name):
        """
        Read yaml properties file
        """
        yaml_props = {}
        with open(file_name, 'r') as stream:
            try:
                yaml_props = yaml.load(stream)
            except yaml.YAMLError as exc:
                print (exc)
        return yaml_props

    @staticmethod
    def hl7_drop_table(segment, db_name):
        """
        Drop table template
        :param segment: hl7 segment name
        :param db_name: database name
        :return: drop table statement string
        """
        # if segment == 'BUI' or segment == 'DON' or segment == 'DPS':
        #     return ''
        # else:
        return '\nDROP TABLE {1}.hl7_{0}_data;\n'.format(segment.lower(), db_name)

    @staticmethod
    def hl7_table_prefix(segment, db_name):
        """
        Prefix for all create table statements
        :param segment: hl7 segment name
        :param db_name: database name
        :return: create table prefix statement strings
        """
        return '\nCREATE EXTERNAL TABLE {1}.hl7_{0} ('.format(segment.lower(), db_name)

    @staticmethod
    def hl7_table_suffix(db_path, message_type):
        """
        The suffix for all create table statements
        :param db_path: hive database name
        :param message_type: HL7 message type
        :return: create table statement suffix string
        """
        return '\n)' \
               '\nPARTITIONED BY (' \
               '\n\tmessage_type STRING,' \
               '\n\ttransaction_date STRING' \
               '\n)' \
               '\nCOMMENT \'Table updated on {2}\'' \
               '\nROW FORMAT DELIMITED' \
               '\nFIELDS TERMINATED BY \'|\'' \
               '\nSTORED AS SEQUENCEFILE' \
               '\nLOCATION \'/user/hive/warehouse/{0}/landing_zone=SEGMENTS/hl7_segment={1}\';\n'.format(
            db_path.lower(),
            message_type,
            datetime.datetime.now().strftime("%Y-%m-%d"))

    @staticmethod
    def clean_comps(comps, index):
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
        if index == 0:
            res = '\t{0} STRING\n'.format(column_name)
        else:
            res = '\t{0} STRING,\n'.format(column_name)
        return res

    @staticmethod
    def add_comment():
        return ''
