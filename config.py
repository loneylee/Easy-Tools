from pyspark import SparkConf
from resources.datasets.tpch import TPCH, DataSetBase

clickhouse_info = {
    'host': "localhost",
    'port': 9000,
    'database': "default",
    'user': 'default',
    'password': ''
}

spark_conf = SparkConf()
spark_conf.set("spark.driver.memory", "10G")
spark_conf.set("spark.memory.fraction", "0.6")
spark_conf.set("spark.memory.storageFraction", "0.3")
spark_conf.set("spark.io.compression.codec", "LZ4")
spark_conf.set("spark.memory.offHeap.enabled", "true")
spark_conf.set("spark.memory.offHeap.size", "20G")
# spark_conf.set("spark.local.dir", "/shuffle/lishuai/tmp");
spark_conf.set("parquet.block.size", "1073741824")

output_dir = "/home/admin123/Downloads"

shards_repartition = {
    "customer": (15, 3),
    "lineitem": (90, 1),
    "nation": (1, 1),
    "orders": (90, 1),
    "part": (15, 3),
    "partsupp": (15, 3),
    "region": (1, 1),
    "supplier": (1, 1)
}

mergetree_dest_dataset: DataSetBase = TPCH("bucket_tpch", use_bucket_=True)


class _ClickhouseConfig(object):
    @property
    def host(self):
        return clickhouse_info['host']

    @property
    def port(self):
        return clickhouse_info['port']

    @property
    def database(self):
        return clickhouse_info['database']

    @property
    def user(self):
        return clickhouse_info['user']

    @property
    def password(self):
        return clickhouse_info['password']


ClickhouseConfig = _ClickhouseConfig()
