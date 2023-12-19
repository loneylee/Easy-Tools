import csv
import os
import random

RULE_START_CONSTANT = "[RULE]"

DEFAULT_INT_LEN = 1000000


class Rule(object):
    def __init__(self):
        self.type = ""
        self.distinct: int = 0
        self.len = 0
        self.value_set = []

    def add_rule(self, pair: str):
        r = pair.split("=")

        if r[0] == "type":
            self.type = r[1]
        elif r[0] == "distinct":
            d = int(r[1])
            if d > 100000:
                d = 100000
            self.distinct = d
        elif r[0] == "len":
            self.len = int(r[1])

    def gen_sample(self):
        index = 0
        while True:
            index = index + 1
            if len(self.value_set) == self.distinct:
                break

            if index > self.distinct * 10:
                break

            self.value_set.append(self._get_value())

    def _get_value(self):
        if self.type == "string":
            if self.len > 0:
                return "".join(random.sample('abcdefghijklmnopqrstuvwxyz!@#$%^&*()中国文化博大精深', self.len))
            else:
                return "".join(
                    random.sample('abcdefghijklmnopqrstuvwxyz!@#$%^&*()中国文化博大精深', random.randint(1, 20)))
        if self.type == "int":
            if self.len > 0:
                return random.randint(DEFAULT_INT_LEN, DEFAULT_INT_LEN * 10 - 1)
            else:
                return random.randint(-DEFAULT_INT_LEN, DEFAULT_INT_LEN)
        if self.type == "float":
            return random.randint(DEFAULT_INT_LEN, DEFAULT_INT_LEN * 10 - 1) / 5

        return ""

    def add_sample(self, v):
        self.value_set.append(v)

    def get_mock(self):
        l = len(self.value_set)
        if l > 0:
            return self.value_set[random.randint(0, l - 1)]
        else:
            return self._get_value()


class Column(object):
    def __init__(self, name_: str):
        self.name = name_
        self.rule = Rule()


def parse_rule(content: str):
    if content.startswith(RULE_START_CONSTANT):
        content = content[len(RULE_START_CONSTANT):]

    pairs = content.split(",")
    rule = Rule()
    for pair in pairs:
        rule.add_rule(pair)

    rule.gen_sample()
    return rule


def read_sample_csv(csv_file: str):
    columns = []

    with open(csv_file, "r", newline='') as f:
        reader = csv.reader(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        row_cnt: int = 0
        for row in reader:
            row_cnt = row_cnt + 1

            if row_cnt == 1:
                for field in row:
                    columns.append(Column(field))
                continue

            if row_cnt == 2:
                for field_num in range(0, len(row)):
                    field = row[field_num]
                    if field is not None and field.startswith(RULE_START_CONSTANT):
                        columns[field_num].rule = parse_rule(field)
                continue

            for field_num in range(0, len(row)):
                field = row[field_num]
                if field is not None and field != "":
                    columns[field_num].rule.add_sample(row[field_num])

    return columns


def write_mock_inner(dirs: str, index: int, columns, total_rows: int, ignore_header):
    with open(dirs + os.sep + str(index) + ".csv", "w", newline='') as f:
        writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for cnt in range(0, total_rows):
            row = []
            for column in columns:
                column: Column
                if cnt == 0 and not ignore_header:
                    row.append(column.name)
                else:
                    row.append(column.rule.get_mock())

            writer.writerow(row)


def write_mock(dirs: str, columns, total_rows: int, max_rows_per_file: int, ignore_header: bool):
    index = 0

    while total_rows > 0:
        if total_rows > max_rows_per_file:
            write_mock_inner(dirs, index, columns, max_rows_per_file, ignore_header)
        else:
            write_mock_inner(dirs, index, columns, total_rows, ignore_header)

        total_rows = total_rows - max_rows_per_file
