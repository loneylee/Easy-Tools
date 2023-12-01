import copy
import os

import pyarrow.parquet as parquet

from commons.client.clickhouse import CHClient
from commons.client.spark import SparkClient
from config import ClickhouseConfig
from resources.sqls.tpch import TPCH, Table, Column, ColumnTypeEnum, ColumnType

tpch_bucket_database = "bucket_tpch"

arrow_type_map = {
    "int64": ColumnTypeEnum.BIGINT,
    "double": ColumnTypeEnum.DOUBLE,
    "string": ColumnTypeEnum.STRING,
    "date32[day]": ColumnTypeEnum.DATE
}

spark_client = SparkClient()


def arrow_type_to_sql_type(type):
    sql_type = arrow_type_map.get(type)

    if sql_type is not None:
        return ColumnType(sql_type, [])

    assert sql_type is not None
    return ColumnType(sql_type, [])


def read_parquet_schema(file: str, table_name_: str) -> Table:
    schema = parquet.read_schema(file)

    table = Table(table_name_)

    for name, t in zip(schema.names, schema.types):
        table.columns.append(Column(name, arrow_type_to_sql_type(t), nullable_=False))

    return table


def parse_table(table_path: str, table_name: str) -> Table:
    files = []
    for file in os.listdir(table_path):
        if file.endswith("parquet"):
            files.append(table_path + os.sep + file)

    if len(files) == 0:
        return None

    sample_file = files[0]
    table = read_parquet_schema(sample_file, table_name)
    table.external_path = table_path
    spark_client.create_table(table)
    print("Load origin parquet table {} success.".format(table.full_name()))
    print(spark_client.execute_and_fetchall("select count(*) from {} limit 10".format(table.full_name())))
    return table


def load_bucket_data(table: Table, tpch, mergetree_path: str):
    mergetree_bucket_path = mergetree_path + os.sep + "clickhouse"

    if table is None or tpch.tables.get(table.name) is None:
        return

    tpch_table: Table = tpch.tables[table.name]
    spark_client.create_table(tpch_table)
    spark_client.execute("insert into {} select * from {}".format(tpch_table.full_name(), table.full_name()))
    print("Load bucket parquet table {} success.".format(table.full_name()))
    print(spark_client.execute_and_fetchall(
        "select count(*) from {} limit 10".format(tpch_table.full_name())))

    bucket_files = {}
    for file in os.listdir(tpch_table.external_path):
        if file.endswith("parquet"):
            bucket_num = file.split(".")[0].split("_")[-1]
            bucket_files.setdefault(bucket_num, [])
            bucket_files[bucket_num].append(tpch_table.external_path + os.sep + file)

    ch = CHClient(ClickhouseConfig)

    for bucket_num in bucket_files.keys():
        tpch_mergetree_table = copy.copy(tpch_table)
        tpch_mergetree_table.name = bucket_num
        tpch_mergetree_table.database = tpch_table.name

        os.system("""
             clickhouse-local --multiquery --query "{query}" \
             --log-level error --logger.console \
             --max_insert_block_size 1048576000 --input_format_parquet_max_block_size 1048576000\
             --path {bucket_path}
             """.format(
            query=";".join(tpch_mergetree_table.to_sql(ch)).replace("\n", " ").replace("`",
                                                                                       "\\`"),
            bucket_path=mergetree_bucket_path))

        index = 0
        for file in bucket_files[bucket_num]:
            index += 1
            bucket_sql = """
                 insert into TABLE {database}.{table_name} SELECT * FROM file('{file}', 'Parquet');
                 """.format(database=tpch_mergetree_table.database, table_name="`" + tpch_mergetree_table.name + "`",
                            file=file)

            os.system("""
            clickhouse-local --multiquery --query "{query}" \
            --log-level error --logger.console \
            --max_insert_block_size 1048576000 --input_format_parquet_max_block_size 1048576000\
            --path {bucket_path}
            """.format(query=bucket_sql.replace("\n", " ").replace("`", "\\`"),
                       bucket_path=mergetree_bucket_path))

            bucket_rel_mergetree_path = tpch_mergetree_table.database + os.sep + tpch_mergetree_table.name
            ch_part_path = mergetree_bucket_path + os.sep + "data" + os.sep + bucket_rel_mergetree_path
            mregetree_part_path = mergetree_path + os.sep + "mergetree" + os.sep + "defalut" + os.sep + bucket_rel_mergetree_path
            part_index = mregetree_part_path + os.sep + "all_" + str(index) + "_" + str(index) + "_0"

            if not os.path.exists(mregetree_part_path):
                os.makedirs(mregetree_part_path)

            os.system("mv {} {}".format(ch_part_path + os.sep + "all_1_1_0", part_index))

            if index == 1:
                os.system("""
                               cp {} {}
                               """.format(
                    ch_part_path + os.sep + "format_version.txt",
                    mregetree_part_path + os.sep
                ))
            print("Load table {} bucket {} file {} part {} Success.".format(tpch_mergetree_table.name, bucket_num, file,
                                                                            part_index))


def parser(ori_path: str, bucket_path: str):
    mergetree_bucket_path: str = bucket_path + "-mergetree"
    tpch = TPCH(tpch_bucket_database, use_bucket_=True, external_path_=bucket_path)

    if not os.path.exists(bucket_path):
        os.makedirs(bucket_path)

    for table_name in os.listdir(ori_path):
        table_path = ori_path + os.sep + table_name
        if os.path.isdir(table_path):
            table = parse_table(table_path, table_name)
            load_bucket_data(table, tpch, mergetree_bucket_path)
