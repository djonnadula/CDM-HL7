import csv
import os
import yaml
import datetime
from constants import *


class ParsingUtils:
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

    @staticmethod
    def get_parent_directory(directory):
        """
        Get the parent directory for a given directory
        :param directory: directory to find the parent of
        :return: parent directory
        """
        return os.path.dirname(directory)

    @staticmethod
    def get_templates_dir(template_path):
        """
        Get the template directory
        :return: location of the templates used for creating segments
        """
        cwd = os.getcwd()
        base_path = ParsingUtils.get_parent_directory(
            ParsingUtils.get_parent_directory(ParsingUtils.get_parent_directory(cwd)))
        return base_path + template_path

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

    """
    Utils class for table creation logic
    """

    @staticmethod
    def create_common_columns(common_columns):
        res = ''
        for ele in common_columns:
            res += NEWLINE + TAB + ele + COMMA
        return res

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
        return NEWLINE + 'DROP TABLE {1}.hl7_{0};'.format(segment.lower(), db_name)

    @staticmethod
    def hl7_table_prefix(segment, db_name):
        """
        Prefix for all create table statements
        :param segment: hl7 segment name
        :param db_name: database name
        :return: create table prefix statement strings
        """
        return NEWLINE + 'CREATE EXTERNAL TABLE {1}.hl7_{0} ('.format(segment.lower(), db_name)

    @staticmethod
    def hl7_table_suffix(db_path, message_type, reason, format_change_time):
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
               '\nCOMMENT \'Update Time: {2} | Reason: {3} | Last Column Format Change Time: {4}\'' \
               '\nROW FORMAT DELIMITED' \
               '\nFIELDS TERMINATED BY \'|\'' \
               '\nSTORED AS SEQUENCEFILE' \
               '\nLOCATION \'/user/hive/warehouse/{0}/landing_zone=SEGMENTS/hl7_segment={1}\';\n'.format(
            db_path.lower(),
            message_type,
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            reason,
            format_change_time)

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
            res = NEWLINE + TAB + '`{0}` STRING'.format(column_name)
        else:
            res = NEWLINE + TAB + '`{0}` STRING,'.format(column_name)
        return res

    @staticmethod
    def table_logic(components, add_drop_tables, segment_name, db_name, db_path, change_reason, drop_file, create_file,
                    last_table_format_change_date, common_columns):
        comp_split = components.split(CARAT)
        size = len(comp_split)
        # print segment_name
        if add_drop_tables:
            drop_file.write(ParsingUtils.hl7_drop_table(segment_name, db_name))
        create_file.write(ParsingUtils.hl7_table_prefix(segment_name, db_name))
        create_file.write(common_columns)
        for element in comp_split:
            size -= 1
            comps = element.split(PIPE)
            cleaned_comps = ParsingUtils.clean_comps(comps, size)
            create_file.write(cleaned_comps)
        create_file.write(
            ParsingUtils.hl7_table_suffix(db_path, segment_name, change_reason, last_table_format_change_date))
