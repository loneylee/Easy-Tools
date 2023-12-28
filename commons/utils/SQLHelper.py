import os
from enum import Enum
import config

class ColumnTypeEnum(Enum):
    BIGINT = 1
    INT = 2
    STRING = 10
    VARCHAR = 11
    CHAR = 12
    DOUBLE = 20
    DATE = 31
    DECIMAL = 40


class Shard(object):
    def __init__(self, columns_: list, shard_num_: int = 1, order_by_=None):
        if order_by_ is None:
            order_by_ = []
        self.columns = columns_
        self.shard_num = shard_num_
        self.order_by = order_by_


class ColumnType(object):
    def __init__(self, type_: ColumnTypeEnum, args=None):
        if args is None:
            args = []
        self.type = type_
        self.args = args


class Column(object):
    def __init__(self, name_, type_: ColumnType, nullable_=False, d=""):
        self.name = name_
        self.type = type_
        self.nullable = nullable_
        self.default = d

    def to_sql(self, engine):
        return "{} {} {} {}".format(self.name, engine.trans_column_type(self.type, self.nullable),
                                    engine.trans_column_nullable(self.nullable),
                                    engine.trans_column_default_value(self.default))


class Table(object):
    def __init__(self, name_, database_: str = "default"):
        self.database = database_
        self.name = name_
        self.columns = []
        self.columnMap = {}
        self.format = ""
        self.order_cols = []
        self.comment = ""
        self.shard_cols: Shard = Shard([])
        self.partition_cols = []
        self.external_path = ""
        self.repartition = 0

    def post_init(self, database, external_path, is_ordered, order_cols, is_bucket, shard_cols):
        self.database = database
        self.repartition = config.shards_repartition.get(self.name)[1]
        self.external_path = external_path + os.sep + self.name
        if is_ordered:
            self.order_cols = order_cols
        if is_bucket:
            self.order_cols = order_cols
            self.shard_cols = Shard(
                shard_cols,
                config.shards_repartition.get(self.name)[0],
                order_cols
            )

    def to_sql(self, engine) -> list:
        sql = []

        external = ""
        if engine.support_external() and self.external_path != "":
            external = "EXTERNAL"

        sql.append("create DATABASE IF NOT EXISTS {}".format(self.database))
        sql.append("drop table if exists {database}.{table_name}"
                   .format(database=self.database, table_name="`" + self.name + "`"))
        sql.append("""CREATE {external} TABLE IF NOT EXISTS {database}.{table_name}
                    (
                    {columns}
                    )
                    {engine}
                    {order_by}
                    {shard_by}
                    {partition_by}
                    {location}
                    {other}""".format(external=external, database=self.database, table_name="`" + self.name + "`",
                                      columns=self._column_to_sql(engine),
                                      engine=engine.engine_sql(self.format),
                                      order_by=engine.order_by_sql(self.order_cols),
                                      shard_by=engine.shard_by_sql(self.shard_cols),
                                      partition_by=engine.partition_by_sql(self.partition_cols),
                                      location=engine.location_sql(self.external_path),
                                      other=engine.other_sql(self)))

        return sql

    def full_name(self) -> str:
        return self.database + "." + self.name

    def _column_to_sql(self, engine):
        sql = ""
        for column in self.columns:
            if sql == "":
                sql = column.to_sql(engine)
            else:
                sql += ",\n" + column.to_sql(engine)
        return sql

    def sql_select_all_column(self):
        res = ""
        for column in self.columns:
            if len(res) == 0:
                res = column.name
            else:
                res += "," + column.name

        return res

    def select_repartition(self):
        if self.repartition > 0:
            return " /*+ repartition({}) */ ".format(self.repartition)

        return ""

    def add_column(self, column: Column):
        self.columns.append(column)
        self.columnMap[column.name] = column
