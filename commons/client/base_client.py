from commons.utils.SQLHelper import Table, ColumnType, Shard


class BaseClient(object):
    def execute(self, stmt):
        pass

    def execute_and_fetchall(self, stmt) -> list:
        pass

    def create_table(self, table: Table):
        for stmt in table.to_sql(self):
            self.execute(stmt)

    def engine_sql(self):
        return ""

    # @abstractmethod
    def order_by_sql(self, order_by_column):
        return ""

    def shard_by_sql(self, shards: Shard):
        return ""

    def partition_by_sql(self, shard_by_column):
        return ""

    def location_sql(self, location_uri):
        return ""

    def other_sql(self, table):
        return ""

    def trans_column_type(self, origin_type: ColumnType):
        return origin_type.type.name

    def trans_column_nullable(self, nullable):
        if nullable:
            return " NULL "
        else:
            return " NOT NULL "

    def trans_column_default_value(self, default_value):
        if default_value == "":
            return ""
        else:
            return " default {} ".format(default_value)

    def support_external(self) -> bool:
        return True
