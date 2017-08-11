from python.parsing_utils.ordered_set import OrderedSet
from python.parsing_utils.segment import Segment
from python.parsing_utils.parsing_utils import ParsingUtils
import csv
from constants import *


def main():
    """
    Main program logic
    Creates both the segments.txt and hl7 impala tables
    """

    yaml_props = ParsingUtils.read_props('create_parsing_data_props.yml')
    segment_file_name = yaml_props.get('segment_file_name')
    static_dict = yaml_props.get('static_dict')
    dup_dict = yaml_props.get('dup_dict')
    file_names = yaml_props.get('file_names')
    underscore_dict = yaml_props.get('underscore_dict')
    skip_dict = yaml_props.get('skip_dict')
    template_path = yaml_props.get('template_path')

    final_string = ''
    current_segment = ''
    templates_dict = {}
    segments_list = []
    adhoc_segments = ""
    adhoc_list = []

    '''
    Segments.txt Logic
    '''
    # Add a 1 or 2 to filenames and create a dictionary so that they are easy to sort
    for filename in file_names:
        with open(ParsingUtils.get_templates_dir(template_path) + filename, 'rU') as csvFile:
            reader = ParsingUtils.unicode_csv_reader(csvFile, delimiter=COMMA)
            reslist = list(reader)
            # Control the order of files so that segments do get added to the middle of the schema
            if filename == STANDARD_MAP:
                templates_dict['1' + filename] = reslist
            # Forgot these in original files
            elif filename == EPIC21_MAP or filename == EPIC231_MAP:
                templates_dict['3' + filename] = reslist
            else:
                templates_dict['2' + filename] = reslist

    # Read segments.txt for ADHOC segments
    with open(ParsingUtils.get_templates_dir(template_path) + SEGMENTS_FILE, 'rU') as segment_file:
        reader = ParsingUtils.unicode_csv_reader(segment_file, delimiter=COMMA)
        for row in reader:
            if row[1].__contains__(ADHOC):
                adhoc_list.append(row)

    joiner = COMMA
    for ele in adhoc_list:
        t = joiner.join(ele)
        adhoc_segments += t + NEWLINE

    message_type_header = adhoc_segments

    # Format data into new list
    for row in templates_dict:
        val = templates_dict.get(row)
        for r in val:
            segment = Segment(source_file=row, index=r[0].strip(), segment_name=r[0].strip().split('.')[0],
                              field=r[1].strip(), component=r[2].strip(), sub_component=r[3].strip())
            segments_list.append(segment)

    # Sort data such that the hl7MapStandard2.8.2.csv is always first
    sorted_segments = sorted(segments_list, key=lambda _segment: (_segment.segment_name, _segment.source_file))

    # let's remove duplicates, but keep order
    final_set = OrderedSet()
    for sorted_tup in sorted_segments:
        final_set.add((sorted_tup.segment_name, sorted_tup.field, sorted_tup.component, sorted_tup.sub_component))

    # Main program logic - writes the formatted lines to segments.txt
    f = open(segment_file_name, 'w')
    for i in final_set:
        skip_flag = 0
        segment_name = i[0]
        field = i[1]
        component = i[2]
        sub_component = i[3]
        if current_segment != segment_name:
            if current_segment != '':
                f.write(ALL + COMMA + final_string + NEWLINE)
            else:
                f.write(message_type_header)
            current_segment = segment_name
            final_string = ''
        for pair in skip_dict:
            split_pair = pair.split(':')
            sp_name = split_pair[0]
            sp_field = split_pair[1]
            if segment_name == sp_name and field == sp_field:
                skip_flag = 1
        if skip_flag == 1:
            continue
        new_string = ParsingUtils.construct_parsing_format(field, component, sub_component)
        if segment_name in underscore_dict:
            new_string = ParsingUtils.add_prefix_underscore(new_string, dup_dict)

        if new_string in static_dict:
            continue
        else:
            if final_string == '':
                final_string = '{0},unknown^{1}'.format(str(current_segment).upper(), new_string)
            else:
                final_string = '{0}^{1}'.format(final_string, new_string)

    f.write(ALL + COMMA + final_string + NEWLINE)
    f.close()

    '''
    Table creation logic
    '''
    environment = yaml_props.get('environment')
    environments = yaml_props.get('environments')
    add_drop_tables = yaml_props.get('add_drop_tables')
    change_reason = yaml_props.get('table_change_reason')
    table_segment = yaml_props.get('table_segments')
    last_table_format_change_date = yaml_props.get('last_table_format_change_date')
    common_columns = ParsingUtils.create_common_columns(yaml_props.get('common_columns'))

    for env in environment:
        db_name = environments.get(env).get('db_name')
        db_path = environments.get(env).get('db_path')
        f = open('sql/create_impala_tables_{0}.sql'.format(env), 'w')
        f2 = open('sql/drop_impala_tables_{0}.sql'.format(env), 'w')
        with open(segment_file_name, 'rU') as csv_file:
            reader = csv.reader(csv_file, delimiter=COMMA)
            for row in reader:
                all_string = row[0]
                segment_name = row[1]
                components = row[2]
                for seg in table_segment:
                    if seg == ALL:
                        if all_string != ALL:
                            continue
                        else:
                            ParsingUtils.table_logic(components, add_drop_tables, segment_name, db_name, db_path,
                                                   change_reason, f2, f, last_table_format_change_date, common_columns)
                else:
                    if all_string != ALL:
                        continue
                    else:
                        for seg in table_segment:
                            if segment_name == seg:
                                ParsingUtils.table_logic(components, add_drop_tables, segment_name, db_name, db_path,
                                                       change_reason, f2, f, last_table_format_change_date,common_columns)
        f2.close()
        f.close()


# Have a main because I'm a Java programmer
if __name__ == "__main__": main()
