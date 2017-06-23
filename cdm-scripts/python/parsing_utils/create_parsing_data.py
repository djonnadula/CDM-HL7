from python.parsing_utils.segment_utils import SegmentUtils
from python.parsing_utils.table_utils import TableUtils
from python.parsing_utils.ordered_set import OrderedSet
from python.parsing_utils.segment import Segment
import csv


def main():
    """
    Main program logic
    Creates both the segments.txt and hl7 impala tables
    """

    yaml_props = TableUtils.read_props('create_parsing_data_props.yml')
    segment_file_name = yaml_props.get('segment_file_name')
    message_type_header = SegmentUtils.adhoc_elements
    cur_message_type = SegmentUtils.default_msg_type_apply

    # Local variables
    final_string = ''
    current_segment = ''
    templates_dict = {}
    segments_list = []
    static_dict = {'etl_firstinsert_datetime', 'field_sequence_num', 'sending_facility', 'message_control_id',
                   'medical_record_num', 'medical_record_urn', 'patient_account_num', 'message_type'}
    dup_dict = {'transaction_date', 'message_control_id'}

    '''
    Segments.txt Logic
    '''
    # Add a 1 or 2 to filenames and create a dictionary so that they are easy to sort
    for filename in SegmentUtils.file_names:
        with open(SegmentUtils.get_templates_dir() + filename, 'rU') as csvFile:
            reader = SegmentUtils.unicode_csv_reader(csvFile, delimiter=',')
            reslist = list(reader)
            if filename == 'hl7MapStandard2.8.2.csv':
                templates_dict['1' + filename] = reslist
            else:
                templates_dict['2' + filename] = reslist

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
                f.write(cur_message_type + ',' + final_string + '\n')
            else:
                f.write(message_type_header)
            current_segment = segment_name
            final_string = ''
        for pair in SegmentUtils.skip_dict:
            split_pair = pair.split(':')
            sp_name = split_pair[0]
            sp_field = split_pair[1]
            if segment_name == sp_name and field == sp_field:
                skip_flag = 1
        if skip_flag == 1:
            continue
        new_string = SegmentUtils.construct_parsing_format(field, component, sub_component)
        if segment_name in SegmentUtils.underscore_dict:
            new_string = SegmentUtils.add_prefix_underscore(new_string, dup_dict)

        if new_string in static_dict:
            continue
        else:
            if final_string == '':
                final_string = '{0},unknown^{1}'.format(str(current_segment).upper(), new_string)
            else:
                final_string = '{0}^{1}'.format(final_string, new_string)

    f.write(cur_message_type + ',' + final_string + '\n')
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

    for env in environment:
        db_name = environments.get(env).get('db_name')
        db_path = environments.get(env).get('db_path')
        f = open('sql/create_impala_tables_{0}.sql'.format(env), 'w')
        f2 = open('sql/drop_impala_tables_{0}.sql'.format(env), 'w')
        with open(segment_file_name, 'rU') as csv_file:
            reader = csv.reader(csv_file, delimiter=',')
            for row in reader:
                all_string = row[0]
                segment_name = row[1]
                components = row[2]
                for seg in table_segment:
                    if seg == 'ALL':
                        if all_string != 'ALL':
                            continue
                        else:
                            TableUtils.table_logic(components, add_drop_tables, segment_name, db_name, db_path,
                                                   change_reason, f2, f, last_table_format_change_date)
                else:
                    if all_string != 'ALL':
                        continue
                    else:
                        for seg in table_segment:
                            if segment_name == seg:
                                TableUtils.table_logic(components, add_drop_tables, segment_name, db_name, db_path,
                                                       change_reason, f2, f, last_table_format_change_date)
        f2.close()
        f.close()


# Have a main because I'm a Java programmer
if __name__ == "__main__": main()
