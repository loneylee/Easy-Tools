import os

import config
from commons.utils.SQLHelper import Table, Column, ColumnType, ColumnTypeEnum, Shard

TPCH_TABLES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]


class TPCH(object):
    def __init__(self, database_: str, nullable_: bool = False, use_decimal_: bool = False, use_bucket_: bool = True,
                 external_path_: str = "", use_orders_: bool = False):
        self.database = database_
        self.nullable = nullable_
        self.use_decimal = use_decimal_  # TODO
        self.use_bucket = use_bucket_
        self.use_orders = use_orders_
        self.tables = {}
        self.external_path = external_path_

        for table_name in TPCH_TABLES:
            self.tables[table_name] = self.__class__.__getattribute__(self, "_" + table_name)()

    def _customer(self):
        name = "customer"
        t_customer = Table(name, self.database)

        t_customer.repartition = config.tpch_shards_repartition.get(name)[1]
        t_customer.external_path = self.external_path + os.sep + name

        t_customer.columns.append(Column("c_custkey", ColumnType(ColumnTypeEnum.BIGINT), self.nullable))
        t_customer.columns.append(Column("c_name", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        t_customer.columns.append(Column("c_address", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        t_customer.columns.append(Column("c_nationkey", ColumnType(ColumnTypeEnum.BIGINT), self.nullable))
        t_customer.columns.append(Column("c_phone", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        t_customer.columns.append(Column("c_acctbal", ColumnType(ColumnTypeEnum.DOUBLE), self.nullable))
        t_customer.columns.append(Column("c_mktsegment", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        t_customer.columns.append(Column("c_comment", ColumnType(ColumnTypeEnum.STRING), self.nullable))

        if self.use_orders:
            t_customer.order_cols = ["c_custkey"]

        if self.use_bucket:
            t_customer.order_cols = ["c_custkey"]
            t_customer.shard_cols = Shard(
                ["c_custkey"],
                config.tpch_shards_repartition.get(name)[0],
                t_customer.order_cols
            )

        return t_customer

    def _lineitem(self):
        name = "lineitem"
        t_lineitem = Table(name, self.database)
        t_lineitem.repartition = config.tpch_shards_repartition.get(name)[1]
        t_lineitem.external_path = self.external_path + os.sep + name
        t_lineitem.columns.append(Column("l_orderkey", ColumnType(ColumnTypeEnum.BIGINT), self.nullable))
        t_lineitem.columns.append(Column("l_partkey", ColumnType(ColumnTypeEnum.BIGINT), self.nullable))
        t_lineitem.columns.append(Column("l_suppkey", ColumnType(ColumnTypeEnum.BIGINT), self.nullable))
        t_lineitem.columns.append(Column("l_linenumber", ColumnType(ColumnTypeEnum.BIGINT), self.nullable))
        t_lineitem.columns.append(Column("l_quantity", ColumnType(ColumnTypeEnum.DOUBLE), self.nullable))
        t_lineitem.columns.append(Column("l_extendedprice", ColumnType(ColumnTypeEnum.DOUBLE), self.nullable))
        t_lineitem.columns.append(Column("l_discount", ColumnType(ColumnTypeEnum.DOUBLE), self.nullable))
        t_lineitem.columns.append(Column("l_tax", ColumnType(ColumnTypeEnum.DOUBLE), self.nullable))
        t_lineitem.columns.append(Column("l_returnflag", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        t_lineitem.columns.append(Column("l_linestatus", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        t_lineitem.columns.append(Column("l_shipdate", ColumnType(ColumnTypeEnum.DATE), self.nullable))
        t_lineitem.columns.append(Column("l_commitdate", ColumnType(ColumnTypeEnum.DATE), self.nullable))
        t_lineitem.columns.append(Column("l_receiptdate", ColumnType(ColumnTypeEnum.DATE), self.nullable))
        t_lineitem.columns.append(Column("l_shipinstruct", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        t_lineitem.columns.append(Column("l_shipmode", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        t_lineitem.columns.append(Column("l_comment", ColumnType(ColumnTypeEnum.STRING), self.nullable))

        if self.use_orders:
            t_lineitem.order_cols = ["l_shipdate", "l_orderkey"]

        if self.use_bucket:
            t_lineitem.order_cols = ["l_shipdate", "l_orderkey"]
            t_lineitem.shard_cols = Shard(
                ["l_orderkey"],
                config.tpch_shards_repartition.get(name)[0],
                t_lineitem.order_cols
            )

        return t_lineitem

    def _nation(self):
        name = "nation"
        t_nation = Table(name, self.database)
        t_nation.repartition = config.tpch_shards_repartition.get(name)[1]
        t_nation.external_path = self.external_path + os.sep + name
        t_nation.columns.append(Column("n_nationkey", ColumnType(ColumnTypeEnum.BIGINT), self.nullable))
        t_nation.columns.append(Column("n_name", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        t_nation.columns.append(Column("n_regionkey", ColumnType(ColumnTypeEnum.BIGINT), self.nullable))
        t_nation.columns.append(Column("n_comment", ColumnType(ColumnTypeEnum.STRING), self.nullable))

        if self.use_orders:
            t_nation.order_cols = ["n_nationkey"]

        if self.use_bucket:
            t_nation.order_cols = ["n_nationkey"]
            t_nation.shard_cols = Shard(
                ["n_nationkey"],
                config.tpch_shards_repartition.get(name)[0],
                t_nation.order_cols
            )

            return t_nation

    def _orders(self):
        name = "orders"
        t_orders = Table(name, self.database)
        t_orders.repartition = config.tpch_shards_repartition.get(name)[1]
        t_orders.external_path = self.external_path + os.sep + name
        t_orders.columns.append(Column("o_orderkey", ColumnType(ColumnTypeEnum.BIGINT), self.nullable))
        t_orders.columns.append(Column("o_custkey", ColumnType(ColumnTypeEnum.BIGINT), self.nullable))
        t_orders.columns.append(Column("o_orderstatus", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        t_orders.columns.append(Column("o_totalprice", ColumnType(ColumnTypeEnum.DOUBLE), self.nullable))
        t_orders.columns.append(Column("o_orderdate", ColumnType(ColumnTypeEnum.DATE), self.nullable))
        t_orders.columns.append(Column("o_orderpriority", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        t_orders.columns.append(Column("o_clerk", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        t_orders.columns.append(Column("o_shippriority", ColumnType(ColumnTypeEnum.BIGINT), self.nullable))
        t_orders.columns.append(Column("o_comment", ColumnType(ColumnTypeEnum.STRING), self.nullable))

        if self.use_orders:
            t_orders.order_cols = ["o_orderkey", "o_orderdate"]

        if self.use_bucket:
            t_orders.order_cols = ["o_orderkey", "o_orderdate"]
            t_orders.shard_cols = Shard(
                ["o_orderkey"],
                config.tpch_shards_repartition.get(name)[0],
                t_orders.order_cols)

        return t_orders

    def _part(self):
        name = "part"
        t_part = Table(name, self.database)
        t_part.repartition = config.tpch_shards_repartition.get(name)[1]
        t_part.external_path = self.external_path + os.sep + name
        t_part.columns.append(Column("p_partkey", ColumnType(ColumnTypeEnum.BIGINT), self.nullable))
        t_part.columns.append(Column("p_name", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        t_part.columns.append(Column("p_mfgr", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        t_part.columns.append(Column("p_brand", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        t_part.columns.append(Column("p_type", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        t_part.columns.append(Column("p_size", ColumnType(ColumnTypeEnum.BIGINT), self.nullable))
        t_part.columns.append(Column("p_container", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        t_part.columns.append(Column("p_retailprice", ColumnType(ColumnTypeEnum.DOUBLE), self.nullable))
        t_part.columns.append(Column("p_comment", ColumnType(ColumnTypeEnum.STRING), self.nullable))

        if self.use_orders:
            t_part.order_cols = ["p_partkey"]

        if self.use_bucket:
            t_part.order_cols = ["p_partkey"]
            t_part.shard_cols = Shard(
                ["p_partkey"],
                config.tpch_shards_repartition.get(name)[0],
                t_part.order_cols
            )

        return t_part

    def _partsupp(self):
        name = "partsupp"
        t_partsupp = Table(name, self.database)
        t_partsupp.repartition = config.tpch_shards_repartition.get(name)[1]
        t_partsupp.external_path = self.external_path + os.sep + name
        t_partsupp.columns.append(Column("ps_partkey", ColumnType(ColumnTypeEnum.BIGINT), self.nullable))
        t_partsupp.columns.append(Column("ps_suppkey", ColumnType(ColumnTypeEnum.BIGINT), self.nullable))
        t_partsupp.columns.append(Column("ps_availqty", ColumnType(ColumnTypeEnum.BIGINT), self.nullable))
        t_partsupp.columns.append(Column("ps_supplycost", ColumnType(ColumnTypeEnum.DOUBLE), self.nullable))
        t_partsupp.columns.append(Column("ps_comment", ColumnType(ColumnTypeEnum.STRING), self.nullable))

        if self.use_orders:
            t_partsupp.order_cols = ["ps_partkey"]

        if self.use_bucket:
            t_partsupp.order_cols = ["ps_partkey"]
            t_partsupp.shard_cols = Shard(
                ["ps_partkey"],
                config.tpch_shards_repartition.get(name)[0],
                t_partsupp.order_cols
            )

        return t_partsupp

    def _region(self):
        name = "region"
        t_region = Table(name, self.database)
        t_region.repartition = config.tpch_shards_repartition.get(name)[1]
        t_region.external_path = self.external_path + os.sep + name
        t_region.columns.append(Column("r_regionkey", ColumnType(ColumnTypeEnum.BIGINT), self.nullable))
        t_region.columns.append(Column("r_name", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        t_region.columns.append(Column("r_comment", ColumnType(ColumnTypeEnum.STRING), self.nullable))

        if self.use_orders:
            t_region.order_cols = ["r_regionkey"]

        if self.use_bucket:
            t_region.order_cols = ["r_regionkey"]
            t_region.shard_cols = Shard(
                ["r_regionkey"],
                config.tpch_shards_repartition.get(name)[0],
                t_region.order_cols
            )

        return t_region

    def _supplier(self):
        name = "supplier"
        t_supplier = Table(name, self.database)
        t_supplier.repartition = config.tpch_shards_repartition.get(name)[1]
        t_supplier.external_path = self.external_path + os.sep + name
        t_supplier.columns.append(Column("s_suppkey", ColumnType(ColumnTypeEnum.BIGINT), self.nullable))
        t_supplier.columns.append(Column("s_name", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        t_supplier.columns.append(Column("s_address", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        t_supplier.columns.append(Column("s_nationkey", ColumnType(ColumnTypeEnum.BIGINT), self.nullable))
        t_supplier.columns.append(Column("s_phone", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        t_supplier.columns.append(Column("s_acctbal", ColumnType(ColumnTypeEnum.DOUBLE), self.nullable))
        t_supplier.columns.append(Column("s_comment", ColumnType(ColumnTypeEnum.STRING), self.nullable))

        if self.use_bucket:
            t_supplier.order_cols = ["s_suppkey"]

        if self.use_bucket:
            t_supplier.order_cols = ["s_suppkey"]
            t_supplier.shard_cols = Shard(
                ["s_suppkey"],
                config.tpch_shards_repartition.get(name)[0],
                t_supplier.order_cols
            )

        return t_supplier
