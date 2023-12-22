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
spark_conf.set("spark.memory.offHeap.size", "20G")
# spark_conf.set("spark.local.dir", "/shuffle/lishuai/tmp");
spark_conf.set("spark.parquet.block.size", "1073741824")

output_dir = "/home/lwz9103/Downloads"

shards_repartition = {
    "store_sales": (90, 3),
    "store_returns": (90, 1),
    "catalog_sales": (90, 3),
    "catalog_returns": (90, 1),
    "web_sales": (90, 3),
    "web_returns": (90, 1),
    "inventory": (90, 3),
    "store": (1, 1),
    "call_center": (1, 1),
    "catalog_page": (15, 1),
    "web_site": (1, 1),
    "web_page": (1, 1),
    "warehouse": (1, 1),
    "customer": (15, 1),
    "customer_address": (15, 1),
    "customer_demographics": (15, 1),
    "date_dim": (15, 1),
    "household_demographics": (1, 1),
    "item": (15, 1),
    "income_band": (1, 1),
    "promotion": (1, 1),
    "reason": (1, 1),
    "ship_mode": (1, 1),
    "time_dim": (15, 1)
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
