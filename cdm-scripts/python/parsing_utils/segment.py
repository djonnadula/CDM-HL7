class Segment:
    """
    Segment class - has 6 attributes
    source_file: which file the segment originated from
    index: hl7 segment type plus index location
    segment_name: hl7 segment type
    field: first part of hl7 segment
    component: second part of hl7 segment
    sub_component: third part of hl7 segment
    """

    def __init__(self, source_file, index, segment_name, field, component, sub_component):
        self.source_file = source_file
        self.index = index
        self.segment_name = segment_name
        self.field = field
        self.component = component
        self.sub_component = sub_component

    def __str__(self):
        return '{0}, {1}, {2}, {3}, {4}, {5}'.format(self.source_file, self.index, self.segment_name, self.field,
                                                     self.component, self.sub_component)

    def __repr__(self):
        return repr((self.source_file, self.index, self.segment_name, self.field, self.component, self.sub_component))

    def __len__(self):
        return len(self)
