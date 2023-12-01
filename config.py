from pyspark import SparkConf

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
spark_conf.set("spark.memory.offHeap.size", "10G")


output_dir = "/home/admin123/Downloads"

tpch_shards_repartition = {
    "customer": (15, 2),
    "lineitem": (90, 3),
    "nation": (1, 1),
    "orders": (90, 1),
    "part": (15, 1),
    "partsupp": (15, 2),
    "region": (1, 1),
    "supplier": (1, 1)
}


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
