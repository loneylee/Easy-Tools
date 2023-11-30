class ProfileData(object):
    __slots__ = ('name', 'event_time_microseconds', 'id', 'plan_step', 'elapsed_us', 'input_wait_elapsed_us',
                 'output_wait_elapsed_us', 'input_rows', 'input_bytes', 'output_rows', 'output_bytes', 'parent_ids',
                 'parent_objs', 'child_objs', 'pname', 'origin_ids', 'step_keys', 'duration')

    def __init__(self, **kwargs):
        for k in kwargs.fromkeys(self.__slots__):
            if k in kwargs:
                setattr(self, k, kwargs[k])

    def to_step_keys(self, datas: list):
        merge_map = {}
        for node in datas:
            merge_map[node.id] = node

        ppids = []
        ppnames = []
        ppstep = []
        for pid in self.parent_ids:
            if pid != '':
                ppids.append(merge_map[pid].parent_ids)
                ppnames.append(merge_map[pid].name)
                ppstep.append(merge_map[pid].plan_step)

        ppids.sort()
        ppnames.sort()
        ppstep.sort()
        setattr(self, 'step_keys', self.name + "_" + self.plan_step + "_" + str(len(self.child_objs)) + "_" + str(
            len(self.parent_ids)) + "_" + str(len(ppids)) + "_" + str(ppnames) + "_" + str(ppstep))

    def get_step_keys(self):
        return self.step_keys

    def to_node(self):
        return self.pname + "\n" \
            + "duration\t" + self.duration + "\n" \
            + "event_time_microseconds\t" + self.event_time_microseconds.strftime("%Y-%m-%d %H:%M:%S.%f") \
                .replace(':', '_') + "\n" \
            + "elapsed\t" + self.elapsed_us + "\n" \
            + "input_wait_elapsed\t" + self.input_wait_elapsed_us + "\n" \
            + "output_wait_elapsed\t" + self.output_wait_elapsed_us + "\n" \
            + "input_rows\t" + self.input_rows + "\n" \
            + "input_bytes\t" + self.input_bytes + "\n" \
            + "output_rows\t" + self.output_rows + "\n" \
            + "output_bytes\t" + self.output_bytes
