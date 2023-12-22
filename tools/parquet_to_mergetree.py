import copy
import csv
import os

import pyarrow.parquet as parquet

from commons.client.clickhouse import CHClient
from commons.client.spark import SparkClient
from commons.utils.SQLHelper import Table, Column, ColumnTypeEnum, ColumnType
from config import ClickhouseConfig
from resources.datasets.dataset import DataSetBase
from resources.datasets.tpcds import TPCDS

arrow_type_map = {
    "int32": ColumnTypeEnum.INT,
    "int64": ColumnTypeEnum.BIGINT,
    "double": ColumnTypeEnum.DOUBLE,
    "string": ColumnTypeEnum.STRING,
    "date32[day]": ColumnTypeEnum.DATE
}

NO_PARTITION_X = "NO_PARTITION_X"

spark_client = SparkClient()


def arrow_type_to_sql_type(type):
    sql_type = arrow_type_map.get(type)

    if sql_type is not None:
        return ColumnType(sql_type, [])

    assert sql_type is not None
    return ColumnType(sql_type, [])


def read_csv_schema(file: str, table_name_: str, dataset: DataSetBase) -> Table:
    table = Table(table_name_)
    table.format = "csv"

    if table is None or dataset.get_tables().get(table.name) is None:
        return table

    dataset_table: Table = dataset.get_tables()[table.name]

    with open(file, "r", newline='') as f:
        reader = csv.reader(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        for row in reader:
            for idx in range(0, len(row)):
                if len(dataset_table.columns) > idx:
                    table.columns.append(dataset_table.columns[idx])
            break
    return table


def read_parquet_schema(file: str, table_name_: str) -> Table:
    schema = parquet.read_schema(file)

    table = Table(table_name_)

    for name, t in zip(schema.names, schema.types):
        table.columns.append(Column(name, arrow_type_to_sql_type(t), nullable_=False))

    return table


def parse_table(table_path: str, table_name: str, ori_format: str, dataset: DataSetBase) -> Table:
    files = []
    for file in os.listdir(table_path):
        if file.endswith(ori_format):
            files.append(table_path + os.sep + file)

    if len(files) == 0:
        return None

    sample_file = files[0]

    if ori_format == "parquet":
        table = read_parquet_schema(sample_file, table_name)
    elif ori_format == "csv":
        table = read_csv_schema(sample_file, table_name, dataset)
    else:
        return None

    table.external_path = table_path
    spark_client.create_table(table)
    print("Load origin {} table {} success.".format(ori_format, table.full_name()))
    print(spark_client.execute_and_fetchall("select count(*) from {}".format(table.full_name())))
    print(spark_client.execute_and_fetchall("select * from {} limit 10".format(table.full_name())))
    return table


def load_bucket_data(table: Table, dataset: DataSetBase, mergetree_path: str):
    mergetree_bucket_path = mergetree_path + os.sep + "clickhouse"

    if table is None or dataset.get_tables().get(table.name) is None:
        return

    dataset_table: Table = dataset.get_tables()[table.name]
    spark_client.create_table(dataset_table)
    insert_sql = "insert into {dest_table_full_name} ({column_names}) select {repartition} {column_names} from {table_fill_name}".format(
            dest_table_full_name=dataset_table.full_name(),
            repartition=dataset_table.select_repartition(),
            column_names=dataset_table.sql_select_all_column(),
            table_fill_name=table.full_name())
    print(insert_sql)
    spark_client.execute(insert_sql)
    print("Load bucket parquet table {} success.".format(table.full_name()))
    print(spark_client.execute_and_fetchall(
        "select count(*) from {} limit 10".format(dataset_table.full_name())))

    bucket_files = {}
    if len(dataset_table.shard_cols.columns) != 0:
        for file in os.listdir(dataset_table.external_path):
            if file.endswith("parquet"):
                bucket_num = file.split(".")[0].split("_")[-1]
                bucket_files.setdefault(bucket_num, [])
                bucket_files[bucket_num].append(dataset_table.external_path + os.sep + file)
    elif len(dataset_table.partition_cols):
        for partition in os.listdir(dataset_table.external_path):
            partition_dir = dataset_table.external_path + os.sep + partition
            if partition.__contains__("=") and os.path.isdir(partition_dir):
                for file in os.listdir(partition_dir):
                    if file.endswith("parquet"):
                        bucket_files.setdefault(partition, [])
                        bucket_files[partition].append(partition_dir + os.sep + file)
    else:
        for file in os.listdir(dataset_table.external_path):
            if file.endswith("parquet"):
                bucket_files.setdefault(NO_PARTITION_X, [])
                bucket_files[NO_PARTITION_X].append(dataset_table.external_path + os.sep + file)

    ch = CHClient(ClickhouseConfig)
    table_name_index = 1

    for bucket_num in bucket_files.keys():
        tpch_mergetree_table = copy.copy(dataset_table)
        tpch_mergetree_table.name = str(table_name_index)
        table_name_index = table_name_index + 1
        tpch_mergetree_table.database = dataset_table.name
        tpch_mergetree_table.partition_cols = []

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
            if len(dataset_table.shard_cols.columns) != 0:
                bucket_sql = """
                                 insert into TABLE {database}.{table_name} SELECT * FROM file('{file}', 'Parquet');
                                 """.format(database=tpch_mergetree_table.database,
                                            table_name="`" + tpch_mergetree_table.name + "`",
                                            file=file)
            elif len(dataset_table.partition_cols):
                p_and_v = bucket_num.split("=", 2)
                bucket_sql = """
                    insert into TABLE {database}.{table_name} ({columns_to}) SELECT {columns_from} FROM file('{file}', 'Parquet');
                                                 """.format(database=tpch_mergetree_table.database,
                                                            table_name="`" + tpch_mergetree_table.name + "`",
                                                            columns_to=dataset_table.sql_select_all_column(),
                                                            columns_from=dataset_table.sql_select_all_column().replace(
                                                                p_and_v[0], "'" + p_and_v[1] + "'"),
                                                            file=file)
            else:
                bucket_sql = """
                                         insert into TABLE {database}.{table_name} SELECT * FROM file('{file}', 'Parquet');
                                         """.format(database=tpch_mergetree_table.database,
                                                    table_name="`" + tpch_mergetree_table.name + "`",
                                                    file=file)

            os.system("""
            clickhouse-local --multiquery --query "{query}" \
            --log-level error --logger.console \
            --max_insert_block_size 10485760000 --input_format_parquet_max_block_size 10485760000\
            --min_insert_block_size_rows 3048576000 --min_insert_block_size_bytes 3048576000\
            --path {bucket_path}
            """.format(query=bucket_sql.replace("\n", " ").replace("`", "\\`"),
                       bucket_path=mergetree_bucket_path))

            ch_part_path = mergetree_bucket_path + os.sep + "data" + os.sep + tpch_mergetree_table.database + os.sep + tpch_mergetree_table.name

            if bucket_num == NO_PARTITION_X:
                mregetree_part_path = mergetree_path + os.sep + "mergetree" + os.sep + "defalut" + os.sep + tpch_mergetree_table.database
            else:
                mregetree_part_path = mergetree_path + os.sep + "mergetree" + os.sep + "defalut" + os.sep + tpch_mergetree_table.database + os.sep + bucket_num

            if not os.path.exists(mregetree_part_path):
                os.makedirs(mregetree_part_path)

            if index == 0:
                os.system("""
                               cp {} {}
                               """.format(
                    ch_part_path + os.sep + "format_version.txt",
                    mregetree_part_path + os.sep
                ))

            for part_file in os.listdir(ch_part_path + os.sep):
                if os.path.isdir(ch_part_path + os.sep + part_file) and part_file.startswith(
                        "all_") and part_file.endswith("_0"):
                    index += 1
                    part_index = mregetree_part_path + os.sep + "all_" + str(index) + "_" + str(index) + "_0"
                    os.system("mv {} {}".format(ch_part_path + os.sep + part_file, part_index))

                    print("Load table {} bucket {} file {} part {} Success.".format(
                        tpch_mergetree_table.name, bucket_num,
                        file,
                        part_index))


def parser(ori_path: str, bucket_path: str, ori_format: str):
    mergetree_bucket_path: str = bucket_path + "-mergetree"
    mergetree_dest_dataset: DataSetBase = TPCDS("bucket_tpcds", use_bucket_=True, external_path_=mergetree_bucket_path)
    if not os.path.exists(bucket_path):
        os.makedirs(bucket_path)

    origin_tables: DataSetBase = TPCDS("default_tpcds", use_orders_=False, use_bucket_=False, external_path_=ori_path)
    for table in origin_tables.get_tables().values():
        spark_client.create_table(table)
        load_bucket_data(table, mergetree_dest_dataset, mergetree_bucket_path)
