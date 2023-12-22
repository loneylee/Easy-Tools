from clickhouse_driver import dbapi

from commons.client.base_client import BaseClient
from commons.utils.SQLHelper import ColumnType, ColumnTypeEnum
from config import ClickhouseConfig

column_type_to_ch_type = {
    ColumnTypeEnum.BIGINT: "Int64",
    ColumnTypeEnum.INT: "Int32",
    ColumnTypeEnum.STRING: "String",
    ColumnTypeEnum.DOUBLE: "Float64",
    ColumnTypeEnum.DATE: "Date32"
}


class CHClient(BaseClient):
    def __init__(self, config: ClickhouseConfig):
        self.connection = dbapi.connect(database=config.database, user=config.user,
                                        password=config.password, host=config.host,
                                        port=config.port)

    def execute_and_fetchall(self, stmt) -> list:
        cursor = self.connection.cursor()
        cursor.execute(stmt)
        result = cursor.fetchall()
        cursor.close()
        return result

    def execute(self, stmt):
        cursor = self.connection.cursor()
        cursor.execute(stmt)
        result = cursor.fetchall()
        cursor.close()
        return result

    def engine_sql(self, fmt: str):
        return " ENGINE=MergeTree() "

    def trans_column_type(self, origin_type: ColumnType):
        t = column_type_to_ch_type.get(origin_type.type)
        if t is not None:
            return t

        if origin_type.type == ColumnTypeEnum.VARCHAR or origin_type.type == ColumnTypeEnum.CHAR:
            return column_type_to_ch_type.get(ColumnTypeEnum.STRING)

        if origin_type.type == ColumnTypeEnum.DECIMAL:
            return "Decimal({},{})".format(origin_type.args[0], origin_type.args[1])

        assert False
        return origin_type.type.name

    def trans_column_nullable(self, nullable):
        return ""

    def order_by_sql(self, order_by_column):
        if order_by_column == "" or len(order_by_column) == 0:
            return " ORDER BY tuple() "

        return " ORDER BY ( " + ",".join(order_by_column) + ") "

    def support_external(self) -> bool:
        return False

    def partition_by_sql(self, shard_by_column):
        if len(shard_by_column) == 0:
            return ""

        return " partition by ({}})".format(",".join(shard_by_column))
