from pyspark import SparkConf
from pyspark.sql import SparkSession

from commons.client.base_client import BaseClient
from commons.utils.SQLHelper import ColumnType, Shard, ColumnTypeEnum
from config import spark_conf

conf = SparkConf()
spark = SparkSession.builder \
    .master("local[*]") \
    .config(conf=spark_conf) \
    .getOrCreate()


class SparkClient(BaseClient):
    def __init__(self):
        pass

    def execute_and_fetchall(self, stmt) -> list:
        df = spark.sql(stmt)
        res = []
        for row in df.collect():
            res.append([str(x) for x in row])

        return res

    def execute(self, stmt):
        spark.sql(stmt)

    def engine_sql(self, fmt: str):
        if fmt == "":
            fmt = "PARQUET"
        return "USING {}".format(fmt.upper())

    def trans_column_nullable(self, nullable):
        if nullable:
            return ""
        else:
            return " NOT NULL "

    def trans_column_type(self, origin_type: ColumnType, nullable=True):
        if len(origin_type.args) == 0:
            return origin_type.type.name
        if origin_type.type == ColumnTypeEnum.VARCHAR:
            return "Varchar({})".format(origin_type.args[0])
        if origin_type.type == ColumnTypeEnum.CHAR:
            return "Char({})".format(origin_type.args[0])
        if origin_type.type == ColumnTypeEnum.DECIMAL:
            return "Decimal({},{})".format(origin_type.args[0], origin_type.args[1])

    def location_sql(self, location_uri):
        if location_uri == "":
            return ""

        return "LOCATION '{}'".format(location_uri)

    def shard_by_sql(self, shard: Shard):
        if len(shard.columns) == 0 or len(shard.order_by) == 0:
            return ""

        return """
        CLUSTERED by ({}) SORTED by  ({}) INTO {} BUCKETS
        """.format(",".join(shard.columns), ",".join(shard.order_by), shard.shard_num)

    def partition_by_sql(self, shard_by_column):
        if len(shard_by_column) == 0:
            return ""

        return " PARTITIONED BY ({})".format(",".join(shard_by_column))
