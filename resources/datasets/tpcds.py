import os

from commons.utils.SQLHelper import Table, Column, ColumnType, ColumnTypeEnum
from resources.datasets.dataset import DataSetBase


TPCDS_TABLES = ["store_sales", "store_returns", "catalog_sales", "catalog_returns", "web_sales", "web_returns", "inventory", "store", "call_center",
         "catalog_page", "web_site", "web_page", "warehouse", "customer", "customer_address", "customer_demographics", "date_dim", "household_demographics",
         "item", "income_band", "promotion", "reason", "ship_mode", "time_dim"]

class TPCDS(DataSetBase):
    def __init__(self, database_: str, nullable_: bool = True, use_decimal_: bool = False, use_bucket_: bool = True,
                 external_path_: str = "", use_orders_: bool = False):
        super().__init__(use_bucket_)
        self.database = database_
        self.nullable = nullable_
        self.use_decimal = use_decimal_  # TODO
        self.use_orders = use_orders_
        self.tables = {}
        self.external_path = external_path_
        self.is_init = False

    def _init_table(self):
        for table_name in TPCDS_TABLES:
            self.tables[table_name] = self.__class__.__getattribute__(self, table_name)()

        self.is_init = True

    def get_tables(self) -> dict:
        if not self.is_init:
            self._init_table()

        return self.tables

    def customer(self):
        name = "customer"
        t_customer = Table(name)
        t_customer.columns.append(Column("c_customer_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_customer.columns.append(Column("c_customer_id", ColumnType(ColumnTypeEnum.CHAR, [16]), self.nullable))
        t_customer.columns.append(Column("c_current_cdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_customer.columns.append(Column("c_current_hdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_customer.columns.append(Column("c_current_addr_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_customer.columns.append(Column("c_first_shipto_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_customer.columns.append(Column("c_first_sales_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_customer.columns.append(Column("c_salutation", ColumnType(ColumnTypeEnum.CHAR, [10]), self.nullable))
        t_customer.columns.append(Column("c_first_name", ColumnType(ColumnTypeEnum.CHAR, [20]), self.nullable))
        t_customer.columns.append(Column("c_last_name", ColumnType(ColumnTypeEnum.CHAR, [30]), self.nullable))
        t_customer.columns.append(Column("c_preferred_cust_flag", ColumnType(ColumnTypeEnum.CHAR, [1]), self.nullable))
        t_customer.columns.append(Column("c_birth_day", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_customer.columns.append(Column("c_birth_month", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_customer.columns.append(Column("c_birth_year", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_customer.columns.append(Column("c_birth_country", ColumnType(ColumnTypeEnum.VARCHAR, [20]), self.nullable))
        t_customer.columns.append(Column("c_login", ColumnType(ColumnTypeEnum.CHAR, [13]), self.nullable))
        t_customer.columns.append(Column("c_email_address", ColumnType(ColumnTypeEnum.CHAR, [50]), self.nullable))
        t_customer.columns.append(Column("c_last_review_date", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_customer.post_init(self.database, self.external_path, self.use_orders, ["c_customer_sk"], self.use_bucket, ["c_customer_sk"])
        return t_customer

    #tpcds
    def store_sales(self):
        t_store_sales = Table("store_sales")
        t_store_sales.columns.append(Column("ss_sold_time_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store_sales.columns.append(Column("ss_item_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store_sales.columns.append(Column("ss_customer_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store_sales.columns.append(Column("ss_cdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store_sales.columns.append(Column("ss_hdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store_sales.columns.append(Column("ss_addr_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store_sales.columns.append(Column("ss_store_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store_sales.columns.append(Column("ss_promo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store_sales.columns.append(Column("ss_ticket_number", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store_sales.columns.append(Column("ss_quantity", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store_sales.columns.append(Column("ss_wholesale_cost", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_store_sales.columns.append(Column("ss_list_price", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))

        t_store_sales.columns.append(Column("ss_sales_price", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_store_sales.columns.append(Column("ss_ext_discount_amt", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_store_sales.columns.append(Column("ss_ext_sales_price", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_store_sales.columns.append(Column("ss_ext_wholesale_cost", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_store_sales.columns.append(Column("ss_ext_list_price", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_store_sales.columns.append(Column("ss_ext_tax", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_store_sales.columns.append(Column("ss_coupon_amt", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))

        t_store_sales.columns.append(Column("ss_net_paid", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_store_sales.columns.append(Column("ss_net_paid_inc_tax", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_store_sales.columns.append(Column("ss_net_profit", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_store_sales.columns.append(Column("ss_sold_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))

        order_cols = ["ss_sold_date_sk", "ss_item_sk", "ss_ticket_number"]
        shard_cols = ["ss_item_sk", "ss_ticket_number"]
        t_store_sales.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)
        return t_store_sales

    def store_returns(self):
        t_store_returns = Table("store_returns")
        t_store_returns.columns.append(Column("sr_return_time_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store_returns.columns.append(Column("sr_item_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store_returns.columns.append(Column("sr_customer_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store_returns.columns.append(Column("sr_cdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store_returns.columns.append(Column("sr_hdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store_returns.columns.append(Column("sr_addr_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_store_returns.columns.append(Column("sr_store_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store_returns.columns.append(Column("sr_reason_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store_returns.columns.append(Column("sr_ticket_number", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store_returns.columns.append(Column("sr_return_quantity", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_store_returns.columns.append(Column("sr_return_amt", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_store_returns.columns.append(Column("sr_return_tax", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_store_returns.columns.append(Column("sr_return_amt_inc_tax", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_store_returns.columns.append(Column("sr_fee", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_store_returns.columns.append(Column("sr_return_ship_cost", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_store_returns.columns.append(Column("sr_refunded_cash", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_store_returns.columns.append(Column("sr_reversed_charge", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_store_returns.columns.append(Column("sr_store_credit", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_store_returns.columns.append(Column("sr_net_loss", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_store_returns.columns.append(Column("sr_returned_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))

        order_cols = ["sr_returned_date_sk", "sr_item_sk", "sr_ticket_number"]
        shard_cols = ["sr_item_sk", "sr_ticket_number"]
        t_store_returns.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)

        return t_store_returns


    def catalog_sales(self):
        t_catalog_sales = Table("catalog_sales")
        t_catalog_sales.columns.append(Column("cs_sold_time_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_sales.columns.append(Column("cs_ship_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_sales.columns.append(Column("cs_bill_customer_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_sales.columns.append(Column("cs_bill_cdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_sales.columns.append(Column("cs_bill_hdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_sales.columns.append(Column("cs_bill_addr_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_catalog_sales.columns.append(Column("cs_ship_customer_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_sales.columns.append(Column("cs_ship_cdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_sales.columns.append(Column("cs_ship_hdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_sales.columns.append(Column("cs_ship_addr_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_sales.columns.append(Column("cs_call_center_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_sales.columns.append(Column("cs_catalog_page_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_catalog_sales.columns.append(Column("cs_ship_mode_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_sales.columns.append(Column("cs_warehouse_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_sales.columns.append(Column("cs_item_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_sales.columns.append(Column("cs_promo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_sales.columns.append(Column("cs_order_number", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_sales.columns.append(Column("cs_quantity", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_catalog_sales.columns.append(Column("cs_wholesale_cost", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_sales.columns.append(Column("cs_list_price", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_sales.columns.append(Column("cs_sales_price", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_sales.columns.append(Column("cs_ext_discount_amt", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_sales.columns.append(Column("cs_ext_sales_price", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_sales.columns.append(Column("cs_ext_wholesale_cost", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))

        t_catalog_sales.columns.append(Column("cs_ext_list_price", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_sales.columns.append(Column("cs_ext_tax", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_sales.columns.append(Column("cs_coupon_amt", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_sales.columns.append(Column("cs_ext_ship_cost", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_sales.columns.append(Column("cs_net_paid", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_sales.columns.append(Column("cs_net_paid_inc_tax", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))

        t_catalog_sales.columns.append(Column("cs_net_paid_inc_ship", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_sales.columns.append(Column("cs_net_paid_inc_ship_tax", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_sales.columns.append(Column("cs_net_profit", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_sales.columns.append(Column("cs_sold_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))

        order_cols = ["cs_sold_date_sk", "cs_item_sk", "cs_order_number"]
        shard_cols = ["cs_item_sk", "cs_order_number"]
        t_catalog_sales.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)

        return t_catalog_sales

    def catalog_returns(self):
        t_catalog_returns = Table("catalog_returns")
        t_catalog_returns.columns.append(Column("cr_returned_time_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_returns.columns.append(Column("cr_item_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_returns.columns.append(Column("cr_refunded_customer_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_returns.columns.append(Column("cr_refunded_cdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_returns.columns.append(Column("cr_refunded_hdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_returns.columns.append(Column("cr_refunded_addr_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_catalog_returns.columns.append(Column("cr_returning_customer_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_returns.columns.append(Column("cr_returning_cdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_returns.columns.append(Column("cr_returning_hdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_returns.columns.append(Column("cr_returning_addr_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_returns.columns.append(Column("cr_call_center_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_returns.columns.append(Column("cr_catalog_page_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_catalog_returns.columns.append(Column("cr_ship_mode_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_returns.columns.append(Column("cr_warehouse_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_returns.columns.append(Column("cr_reason_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_returns.columns.append(Column("cr_order_number", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_returns.columns.append(Column("cr_return_quantity", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_catalog_returns.columns.append(Column("cr_return_amount", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_returns.columns.append(Column("cr_return_tax", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_returns.columns.append(Column("cr_return_amt_inc_tax", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_returns.columns.append(Column("cr_fee", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_returns.columns.append(Column("cr_return_ship_cost", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_returns.columns.append(Column("cr_refunded_cash", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_returns.columns.append(Column("cr_reversed_charge", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_returns.columns.append(Column("cr_store_credit", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_returns.columns.append(Column("cr_net_loss", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_catalog_returns.columns.append(Column("cr_returned_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))

        order_cols = ["cr_returned_date_sk", "cr_item_sk", "cr_order_number"]
        shard_cols = ["cr_item_sk", "cr_order_number"]
        t_catalog_returns.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)
        return t_catalog_returns


    def web_sales(self):
        t_web_sales = Table("web_sales")
        t_web_sales.columns.append(Column("ws_sold_time_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_sales.columns.append(Column("ws_ship_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_sales.columns.append(Column("ws_item_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_sales.columns.append(Column("ws_bill_customer_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_sales.columns.append(Column("ws_bill_cdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_sales.columns.append(Column("ws_bill_hdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_web_sales.columns.append(Column("ws_bill_addr_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_sales.columns.append(Column("ws_ship_customer_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_sales.columns.append(Column("ws_ship_cdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_sales.columns.append(Column("ws_ship_hdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_sales.columns.append(Column("ws_ship_addr_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_sales.columns.append(Column("ws_web_page_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_web_sales.columns.append(Column("ws_web_site_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_sales.columns.append(Column("ws_ship_mode_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_sales.columns.append(Column("ws_warehouse_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_sales.columns.append(Column("ws_promo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_sales.columns.append(Column("ws_order_number", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_sales.columns.append(Column("ws_quantity", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_web_sales.columns.append(Column("ws_wholesale_cost", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_sales.columns.append(Column("ws_list_price", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_sales.columns.append(Column("ws_sales_price", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_sales.columns.append(Column("ws_ext_discount_amt", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_sales.columns.append(Column("ws_ext_sales_price", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_sales.columns.append(Column("ws_ext_wholesale_cost", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))

        t_web_sales.columns.append(Column("ws_ext_list_price", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_sales.columns.append(Column("ws_ext_tax", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_sales.columns.append(Column("ws_coupon_amt", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_sales.columns.append(Column("ws_ext_ship_cost", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_sales.columns.append(Column("ws_net_paid", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_sales.columns.append(Column("ws_net_paid_inc_tax", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))

        t_web_sales.columns.append(Column("ws_net_paid_inc_ship", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_sales.columns.append(Column("ws_net_paid_inc_ship_tax", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_sales.columns.append(Column("ws_net_profit", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_sales.columns.append(Column("ws_sold_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))

        order_cols = ["ws_sold_date_sk", "ws_item_sk", "ws_order_number"]
        shard_cols = ["ws_item_sk", "ws_order_number"]
        t_web_sales.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)
        return t_web_sales


    def web_returns(self):
        t_web_returns = Table("web_returns")
        t_web_returns.columns.append(Column("wr_returned_time_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_returns.columns.append(Column("wr_item_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_returns.columns.append(Column("wr_refunded_customer_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_returns.columns.append(Column("wr_refunded_cdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_returns.columns.append(Column("wr_refunded_hdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_returns.columns.append(Column("wr_refunded_addr_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_web_returns.columns.append(Column("wr_returning_customer_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_returns.columns.append(Column("wr_returning_cdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_returns.columns.append(Column("wr_returning_hdemo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_returns.columns.append(Column("wr_returning_addr_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_returns.columns.append(Column("wr_web_page_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_returns.columns.append(Column("wr_reason_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_returns.columns.append(Column("wr_order_number", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_returns.columns.append(Column("wr_return_quantity", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_web_returns.columns.append(Column("wr_return_amt", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_returns.columns.append(Column("wr_return_tax", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_returns.columns.append(Column("wr_return_amt_inc_tax", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_returns.columns.append(Column("wr_fee", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_returns.columns.append(Column("wr_return_ship_cost", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_returns.columns.append(Column("wr_refunded_cash", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_returns.columns.append(Column("wr_reversed_charge", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_returns.columns.append(Column("wr_account_credit", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_returns.columns.append(Column("wr_net_loss", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_web_returns.columns.append(Column("wr_returned_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))

        order_cols = ["wr_returned_date_sk", "wr_item_sk", "wr_order_number"]
        shard_cols = ["wr_item_sk", "wr_order_number"]
        t_web_returns.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)
        return t_web_returns

    def inventory(self):
        t_inventory = Table("inventory")
        t_inventory.columns.append(Column("inv_item_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_inventory.columns.append(Column("inv_warehouse_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_inventory.columns.append(Column("inv_quantity_on_hand", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_inventory.columns.append(Column("inv_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))

        order_cols = ["inv_date_sk", "inv_item_sk", "inv_warehouse_sk"]
        shard_cols = ["inv_item_sk"]
        t_inventory.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)
        return t_inventory


    def store(self):
        t_store = Table("store")
        t_store.columns.append(Column("s_store_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store.columns.append(Column("s_store_id", ColumnType(ColumnTypeEnum.CHAR, [16]), self.nullable))
        t_store.columns.append(Column("s_rec_start_date", ColumnType(ColumnTypeEnum.DATE), self.nullable))
        t_store.columns.append(Column("s_rec_end_date", ColumnType(ColumnTypeEnum.DATE), self.nullable))

        t_store.columns.append(Column("s_closed_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store.columns.append(Column("s_store_name", ColumnType(ColumnTypeEnum.VARCHAR, [50]), self.nullable))
        t_store.columns.append(Column("s_number_employees", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store.columns.append(Column("s_floor_space", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_store.columns.append(Column("s_hours", ColumnType(ColumnTypeEnum.CHAR, [20]), self.nullable))
        t_store.columns.append(Column("s_manager", ColumnType(ColumnTypeEnum.VARCHAR, [40]), self.nullable))
        t_store.columns.append(Column("s_market_id", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store.columns.append(Column("s_geography_class", ColumnType(ColumnTypeEnum.VARCHAR, [100]), self.nullable))

        t_store.columns.append(Column("s_market_desc", ColumnType(ColumnTypeEnum.VARCHAR, [100]), self.nullable))
        t_store.columns.append(Column("s_market_manager", ColumnType(ColumnTypeEnum.VARCHAR, [40]), self.nullable))
        t_store.columns.append(Column("s_division_id", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store.columns.append(Column("s_division_name", ColumnType(ColumnTypeEnum.VARCHAR, [50]), self.nullable))

        t_store.columns.append(Column("s_company_id", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_store.columns.append(Column("s_company_name", ColumnType(ColumnTypeEnum.VARCHAR, [50]), self.nullable))
        t_store.columns.append(Column("s_street_number", ColumnType(ColumnTypeEnum.VARCHAR, [10]), self.nullable))
        t_store.columns.append(Column("s_street_name", ColumnType(ColumnTypeEnum.VARCHAR, [60]), self.nullable))

        t_store.columns.append(Column("s_street_type", ColumnType(ColumnTypeEnum.CHAR, [15]), self.nullable))
        t_store.columns.append(Column("s_suite_number", ColumnType(ColumnTypeEnum.CHAR, [10]), self.nullable))
        t_store.columns.append(Column("s_city", ColumnType(ColumnTypeEnum.VARCHAR, [60]), self.nullable))
        t_store.columns.append(Column("s_county", ColumnType(ColumnTypeEnum.VARCHAR, [30]), self.nullable))

        t_store.columns.append(Column("s_state", ColumnType(ColumnTypeEnum.CHAR, [2]), self.nullable))
        t_store.columns.append(Column("s_zip", ColumnType(ColumnTypeEnum.CHAR, [10]), self.nullable))
        t_store.columns.append(Column("s_country", ColumnType(ColumnTypeEnum.VARCHAR, [20]), self.nullable))
        t_store.columns.append(Column("s_gmt_offset", ColumnType(ColumnTypeEnum.DECIMAL, [5, 2]), self.nullable))
        t_store.columns.append(Column("s_tax_percentage", ColumnType(ColumnTypeEnum.DECIMAL, [5, 2]), self.nullable))

        order_cols = ["s_store_sk"]
        shard_cols = ["s_store_sk"]
        t_store.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)
        return t_store

    def call_center(self):
        t_call_center = Table("call_center")
        t_call_center.columns.append(Column("cc_call_center_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_call_center.columns.append(Column("cc_call_center_id", ColumnType(ColumnTypeEnum.CHAR, [16]), self.nullable))
        t_call_center.columns.append(Column("cc_rec_start_date", ColumnType(ColumnTypeEnum.DATE), self.nullable))
        t_call_center.columns.append(Column("cc_rec_end_date", ColumnType(ColumnTypeEnum.DATE), self.nullable))

        t_call_center.columns.append(Column("cc_closed_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_call_center.columns.append(Column("cc_open_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_call_center.columns.append(Column("cc_name", ColumnType(ColumnTypeEnum.VARCHAR, [50]), self.nullable))
        t_call_center.columns.append(Column("cc_class", ColumnType(ColumnTypeEnum.VARCHAR, [50]), self.nullable))

        t_call_center.columns.append(Column("cc_employees", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_call_center.columns.append(Column("cc_sq_ft", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_call_center.columns.append(Column("cc_hours", ColumnType(ColumnTypeEnum.CHAR, [20]), self.nullable))
        t_call_center.columns.append(Column("cc_manager", ColumnType(ColumnTypeEnum.VARCHAR, [40]), self.nullable))

        t_call_center.columns.append(Column("cc_mkt_id", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_call_center.columns.append(Column("cc_mkt_class", ColumnType(ColumnTypeEnum.CHAR, [50]), self.nullable))
        t_call_center.columns.append(Column("cc_mkt_desc", ColumnType(ColumnTypeEnum.VARCHAR, [100]), self.nullable))
        t_call_center.columns.append(Column("cc_market_manager", ColumnType(ColumnTypeEnum.VARCHAR, [40]), self.nullable))

        t_call_center.columns.append(Column("cc_division", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_call_center.columns.append(Column("cc_division_name", ColumnType(ColumnTypeEnum.VARCHAR, [50]), self.nullable))
        t_call_center.columns.append(Column("cc_company", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_call_center.columns.append(Column("cc_company_name", ColumnType(ColumnTypeEnum.CHAR, [50]), self.nullable))

        t_call_center.columns.append(Column("cc_street_number", ColumnType(ColumnTypeEnum.CHAR, [10]), self.nullable))
        t_call_center.columns.append(Column("cc_street_name", ColumnType(ColumnTypeEnum.VARCHAR, [60]), self.nullable))
        t_call_center.columns.append(Column("cc_street_type", ColumnType(ColumnTypeEnum.CHAR, [15]), self.nullable))
        t_call_center.columns.append(Column("cc_suite_number", ColumnType(ColumnTypeEnum.CHAR, [10]), self.nullable))

        t_call_center.columns.append(Column("cc_city", ColumnType(ColumnTypeEnum.VARCHAR, [60]), self.nullable))
        t_call_center.columns.append(Column("cc_county", ColumnType(ColumnTypeEnum.VARCHAR, [30]), self.nullable))
        t_call_center.columns.append(Column("cc_state", ColumnType(ColumnTypeEnum.CHAR, [2]), self.nullable))
        t_call_center.columns.append(Column("cc_zip", ColumnType(ColumnTypeEnum.CHAR, [10]), self.nullable))

        t_call_center.columns.append(Column("cc_country", ColumnType(ColumnTypeEnum.VARCHAR, [20]), self.nullable))
        t_call_center.columns.append(Column("cc_gmt_offset", ColumnType(ColumnTypeEnum.DECIMAL, [5, 2]), self.nullable))
        t_call_center.columns.append(Column("cc_tax_percentage", ColumnType(ColumnTypeEnum.DECIMAL, [5, 2]), self.nullable))

        order_cols = ["cc_call_center_sk"]
        shard_cols = ["cc_call_center_sk"]
        t_call_center.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)
        return t_call_center

    def catalog_page(self):
        t_catalog_page = Table("catalog_page")
        t_catalog_page.columns.append(Column("cp_catalog_page_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_page.columns.append(Column("cp_catalog_page_id", ColumnType(ColumnTypeEnum.CHAR, [16]), self.nullable))
        t_catalog_page.columns.append(Column("cp_start_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_page.columns.append(Column("cp_end_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_catalog_page.columns.append(Column("cp_department", ColumnType(ColumnTypeEnum.VARCHAR, [50]), self.nullable))
        t_catalog_page.columns.append(Column("cp_catalog_number", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_page.columns.append(Column("cp_catalog_page_number", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_catalog_page.columns.append(Column("cp_description", ColumnType(ColumnTypeEnum.VARCHAR, [100]), self.nullable))
        t_catalog_page.columns.append(Column("cp_type", ColumnType(ColumnTypeEnum.VARCHAR, [100]), self.nullable))

        order_cols = ["cp_catalog_page_sk"]
        shard_cols = ["cp_catalog_page_sk"]
        t_catalog_page.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)
        return t_catalog_page

    def web_site(self):
        t_web_site = Table("web_site")
        t_web_site.columns.append(Column("web_site_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_site.columns.append(Column("web_site_id", ColumnType(ColumnTypeEnum.CHAR, [16]), self.nullable))
        t_web_site.columns.append(Column("web_rec_start_date", ColumnType(ColumnTypeEnum.DATE), self.nullable))
        t_web_site.columns.append(Column("web_rec_end_date", ColumnType(ColumnTypeEnum.DATE), self.nullable))

        t_web_site.columns.append(Column("web_name", ColumnType(ColumnTypeEnum.VARCHAR, [50]), self.nullable))
        t_web_site.columns.append(Column("web_open_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_site.columns.append(Column("web_close_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_site.columns.append(Column("web_class", ColumnType(ColumnTypeEnum.VARCHAR, [50]), self.nullable))

        t_web_site.columns.append(Column("web_manager", ColumnType(ColumnTypeEnum.VARCHAR, [40]), self.nullable))
        t_web_site.columns.append(Column("web_mkt_id", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_site.columns.append(Column("web_mkt_class", ColumnType(ColumnTypeEnum.VARCHAR, [50]), self.nullable))
        t_web_site.columns.append(Column("web_mkt_desc", ColumnType(ColumnTypeEnum.VARCHAR, [100]), self.nullable))

        t_web_site.columns.append(Column("web_market_manager", ColumnType(ColumnTypeEnum.VARCHAR, [40]), self.nullable))
        t_web_site.columns.append(Column("web_company_id", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_site.columns.append(Column("web_company_name", ColumnType(ColumnTypeEnum.CHAR, [50]), self.nullable))
        t_web_site.columns.append(Column("web_street_number", ColumnType(ColumnTypeEnum.CHAR, [10]), self.nullable))

        t_web_site.columns.append(Column("web_street_name", ColumnType(ColumnTypeEnum.VARCHAR, [60]), self.nullable))
        t_web_site.columns.append(Column("web_street_type", ColumnType(ColumnTypeEnum.CHAR, [15]), self.nullable))
        t_web_site.columns.append(Column("web_suite_number", ColumnType(ColumnTypeEnum.CHAR, [10]), self.nullable))
        t_web_site.columns.append(Column("web_city", ColumnType(ColumnTypeEnum.VARCHAR, [60]), self.nullable))

        t_web_site.columns.append(Column("web_county", ColumnType(ColumnTypeEnum.VARCHAR, [30]), self.nullable))
        t_web_site.columns.append(Column("web_state", ColumnType(ColumnTypeEnum.CHAR, [2]), self.nullable))
        t_web_site.columns.append(Column("web_zip", ColumnType(ColumnTypeEnum.CHAR, [10]), self.nullable))
        t_web_site.columns.append(Column("web_country", ColumnType(ColumnTypeEnum.VARCHAR, [20]), self.nullable))
        t_web_site.columns.append(Column("web_gmt_offset", ColumnType(ColumnTypeEnum.DECIMAL, [5, 2]), self.nullable))
        t_web_site.columns.append(Column("web_tax_percentage", ColumnType(ColumnTypeEnum.DECIMAL, [5, 2]), self.nullable))

        order_cols = ["web_site_sk"]
        shard_cols = ["web_site_sk"]
        t_web_site.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)
        return t_web_site

    def web_page(self):
        t_web_page = Table("web_page")
        t_web_page.columns.append(Column("wp_web_page_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_page.columns.append(Column("wp_web_page_id", ColumnType(ColumnTypeEnum.CHAR, [16]), self.nullable))
        t_web_page.columns.append(Column("wp_rec_start_date", ColumnType(ColumnTypeEnum.DATE), self.nullable))
        t_web_page.columns.append(Column("wp_rec_end_date", ColumnType(ColumnTypeEnum.DATE), self.nullable))

        t_web_page.columns.append(Column("wp_creation_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_page.columns.append(Column("wp_access_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_page.columns.append(Column("wp_autogen_flag", ColumnType(ColumnTypeEnum.CHAR, [1]), self.nullable))
        t_web_page.columns.append(Column("wp_customer_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_web_page.columns.append(Column("wp_url", ColumnType(ColumnTypeEnum.VARCHAR, [100]), self.nullable))
        t_web_page.columns.append(Column("wp_type", ColumnType(ColumnTypeEnum.CHAR, [50]), self.nullable))
        t_web_page.columns.append(Column("wp_char_count", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_page.columns.append(Column("wp_link_count", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_page.columns.append(Column("wp_image_count", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_web_page.columns.append(Column("wp_max_ad_count", ColumnType(ColumnTypeEnum.INT), self.nullable))

        order_cols = ["wp_web_page_sk"]
        shard_cols = ["wp_web_page_sk"]
        t_web_page.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)
        return t_web_page

    def warehouse(self):
        t_warehouse = Table("warehouse")
        t_warehouse.columns.append(Column("w_warehouse_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_warehouse.columns.append(Column("w_warehouse_id", ColumnType(ColumnTypeEnum.CHAR, [16]), self.nullable))
        t_warehouse.columns.append(Column("w_warehouse_name", ColumnType(ColumnTypeEnum.VARCHAR, [20]), self.nullable))
        t_warehouse.columns.append(Column("w_warehouse_sq_ft", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_warehouse.columns.append(Column("w_street_number", ColumnType(ColumnTypeEnum.CHAR, [10]), self.nullable))
        t_warehouse.columns.append(Column("w_street_name", ColumnType(ColumnTypeEnum.VARCHAR, [20]), self.nullable))
        t_warehouse.columns.append(Column("w_street_type", ColumnType(ColumnTypeEnum.CHAR, [15]), self.nullable))
        t_warehouse.columns.append(Column("w_suite_number", ColumnType(ColumnTypeEnum.CHAR, [10]), self.nullable))

        t_warehouse.columns.append(Column("w_city", ColumnType(ColumnTypeEnum.VARCHAR, [60]), self.nullable))
        t_warehouse.columns.append(Column("w_county", ColumnType(ColumnTypeEnum.VARCHAR, [30]), self.nullable))
        t_warehouse.columns.append(Column("w_state", ColumnType(ColumnTypeEnum.CHAR, [2]), self.nullable))
        t_warehouse.columns.append(Column("w_zip", ColumnType(ColumnTypeEnum.CHAR, [10]), self.nullable))
        t_warehouse.columns.append(Column("w_country", ColumnType(ColumnTypeEnum.VARCHAR, [20]), self.nullable))
        t_warehouse.columns.append(Column("w_gmt_offset", ColumnType(ColumnTypeEnum.DECIMAL, [5, 2]), self.nullable))

        order_cols = ["w_warehouse_sk"]
        shard_cols = ["w_warehouse_sk"]
        t_warehouse.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)
        return t_warehouse

    def customer_address(self):
        t_customer_address = Table("customer_address")
        t_customer_address.columns.append(Column("ca_address_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_customer_address.columns.append(Column("ca_address_id", ColumnType(ColumnTypeEnum.CHAR, [16]), self.nullable))
        t_customer_address.columns.append(Column("ca_street_number", ColumnType(ColumnTypeEnum.CHAR, [10]), self.nullable))
        t_customer_address.columns.append(Column("ca_street_name", ColumnType(ColumnTypeEnum.VARCHAR, [60]), self.nullable))

        t_customer_address.columns.append(Column("ca_street_type", ColumnType(ColumnTypeEnum.CHAR, [15]), self.nullable))
        t_customer_address.columns.append(Column("ca_suite_number", ColumnType(ColumnTypeEnum.CHAR, [10]), self.nullable))
        t_customer_address.columns.append(Column("ca_city", ColumnType(ColumnTypeEnum.VARCHAR, [60]), self.nullable))
        t_customer_address.columns.append(Column("ca_county", ColumnType(ColumnTypeEnum.VARCHAR, [30]), self.nullable))

        t_customer_address.columns.append(Column("ca_state", ColumnType(ColumnTypeEnum.CHAR, [2]), self.nullable))
        t_customer_address.columns.append(Column("ca_zip", ColumnType(ColumnTypeEnum.CHAR, [10]), self.nullable))
        t_customer_address.columns.append(Column("ca_country", ColumnType(ColumnTypeEnum.VARCHAR, [20]), self.nullable))
        t_customer_address.columns.append(Column("ca_gmt_offset", ColumnType(ColumnTypeEnum.DECIMAL, [5, 2]), self.nullable))
        t_customer_address.columns.append(Column("ca_location_type", ColumnType(ColumnTypeEnum.CHAR, [20]), self.nullable))

        order_cols = ["ca_address_sk"]
        shard_cols = ["ca_address_sk"]
        t_customer_address.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)
        return t_customer_address


    def customer_demographics(self):
        t_customer_demographics = Table("customer_demographics")
        t_customer_demographics.columns.append(Column("cd_demo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_customer_demographics.columns.append(Column("cd_gender", ColumnType(ColumnTypeEnum.CHAR, [1]), self.nullable))
        t_customer_demographics.columns.append(Column("cd_marital_status", ColumnType(ColumnTypeEnum.CHAR, [1]), self.nullable))
        t_customer_demographics.columns.append(Column("cd_education_status", ColumnType(ColumnTypeEnum.CHAR, [20]), self.nullable))

        t_customer_demographics.columns.append(Column("cd_purchase_estimate", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_customer_demographics.columns.append(Column("cd_credit_rating", ColumnType(ColumnTypeEnum.CHAR, [10]), self.nullable))
        t_customer_demographics.columns.append(Column("cd_dep_count", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_customer_demographics.columns.append(Column("cd_dep_employed_count", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_customer_demographics.columns.append(Column("cd_dep_college_count", ColumnType(ColumnTypeEnum.INT), self.nullable))

        order_cols = ["cd_demo_sk"]
        shard_cols = ["cd_demo_sk"]
        t_customer_demographics.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)
        return t_customer_demographics

    def date_dim(self):
        t_date_dim = Table("date_dim")
        t_date_dim.columns.append(Column("d_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_date_dim.columns.append(Column("d_date_id", ColumnType(ColumnTypeEnum.CHAR, [16]), self.nullable))
        t_date_dim.columns.append(Column("d_date", ColumnType(ColumnTypeEnum.DATE), self.nullable))
        t_date_dim.columns.append(Column("d_month_seq", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_date_dim.columns.append(Column("d_week_seq", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_date_dim.columns.append(Column("d_quarter_seq", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_date_dim.columns.append(Column("d_year", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_date_dim.columns.append(Column("d_dow", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_date_dim.columns.append(Column("d_moy", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_date_dim.columns.append(Column("d_dom", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_date_dim.columns.append(Column("d_qoy", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_date_dim.columns.append(Column("d_fy_year", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_date_dim.columns.append(Column("d_fy_quarter_seq", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_date_dim.columns.append(Column("d_fy_week_seq", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_date_dim.columns.append(Column("d_day_name", ColumnType(ColumnTypeEnum.CHAR, [9]), self.nullable))
        t_date_dim.columns.append(Column("d_quarter_name", ColumnType(ColumnTypeEnum.CHAR, [6]), self.nullable))
        t_date_dim.columns.append(Column("d_holiday", ColumnType(ColumnTypeEnum.CHAR, [1]), self.nullable))
        t_date_dim.columns.append(Column("d_weekend", ColumnType(ColumnTypeEnum.CHAR, [1]), self.nullable))

        t_date_dim.columns.append(Column("d_following_holiday", ColumnType(ColumnTypeEnum.CHAR, [1]), self.nullable))
        t_date_dim.columns.append(Column("d_first_dom", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_date_dim.columns.append(Column("d_last_dom", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_date_dim.columns.append(Column("d_same_day_ly", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_date_dim.columns.append(Column("d_same_day_lq", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_date_dim.columns.append(Column("d_current_day", ColumnType(ColumnTypeEnum.CHAR, [1]), self.nullable))
        t_date_dim.columns.append(Column("d_current_week", ColumnType(ColumnTypeEnum.CHAR, [1]), self.nullable))
        t_date_dim.columns.append(Column("d_current_month", ColumnType(ColumnTypeEnum.CHAR, [1]), self.nullable))
        t_date_dim.columns.append(Column("d_current_quarter", ColumnType(ColumnTypeEnum.CHAR, [1]), self.nullable))
        t_date_dim.columns.append(Column("d_current_year", ColumnType(ColumnTypeEnum.CHAR, [1]), self.nullable))

        order_cols = ["d_date_sk"]
        shard_cols = ["d_date_sk"]
        t_date_dim.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)
        return t_date_dim

    def household_demographics(self):
        t_household_demographics = Table("household_demographics")
        t_household_demographics.columns.append(Column("hd_demo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_household_demographics.columns.append(Column("hd_income_band_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_household_demographics.columns.append(Column("hd_buy_potential", ColumnType(ColumnTypeEnum.CHAR, [15]), self.nullable))
        t_household_demographics.columns.append(Column("hd_dep_count", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_household_demographics.columns.append(Column("hd_vehicle_count", ColumnType(ColumnTypeEnum.INT), self.nullable))

        order_cols = ["hd_demo_sk"]
        shard_cols = ["hd_demo_sk"]
        t_household_demographics.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)
        return t_household_demographics

    def item(self):
        t_item = Table("item")
        t_item.columns.append(Column("i_item_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_item.columns.append(Column("i_item_id", ColumnType(ColumnTypeEnum.CHAR, [16]), self.nullable))
        t_item.columns.append(Column("i_rec_start_date", ColumnType(ColumnTypeEnum.DATE), self.nullable))
        t_item.columns.append(Column("i_rec_end_date", ColumnType(ColumnTypeEnum.DATE), self.nullable))

        t_item.columns.append(Column("i_item_desc", ColumnType(ColumnTypeEnum.VARCHAR, [200]), self.nullable))
        t_item.columns.append(Column("i_current_price", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_item.columns.append(Column("i_wholesale_cost", ColumnType(ColumnTypeEnum.DECIMAL, [7, 2]), self.nullable))
        t_item.columns.append(Column("i_brand_id", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_item.columns.append(Column("i_brand", ColumnType(ColumnTypeEnum.CHAR, [50]), self.nullable))
        t_item.columns.append(Column("i_class_id", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_item.columns.append(Column("i_class", ColumnType(ColumnTypeEnum.CHAR, [50]), self.nullable))
        t_item.columns.append(Column("i_category_id", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_item.columns.append(Column("i_category", ColumnType(ColumnTypeEnum.CHAR, [50]), self.nullable))
        t_item.columns.append(Column("i_manufact_id", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_item.columns.append(Column("i_manufact", ColumnType(ColumnTypeEnum.CHAR, [50]), self.nullable))
        t_item.columns.append(Column("i_size", ColumnType(ColumnTypeEnum.CHAR, [20]), self.nullable))

        t_item.columns.append(Column("i_formulation", ColumnType(ColumnTypeEnum.CHAR, [20]), self.nullable))
        t_item.columns.append(Column("i_color", ColumnType(ColumnTypeEnum.CHAR, [20]), self.nullable))
        t_item.columns.append(Column("i_units", ColumnType(ColumnTypeEnum.CHAR, [10]), self.nullable))
        t_item.columns.append(Column("i_container", ColumnType(ColumnTypeEnum.CHAR, [10]), self.nullable))
        t_item.columns.append(Column("i_manager_id", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_item.columns.append(Column("i_product_name", ColumnType(ColumnTypeEnum.CHAR, [50]), self.nullable))

        order_cols = ["i_item_sk"]
        shard_cols = ["i_item_sk"]
        t_item.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)
        return t_item

    def income_band(self):
        t_income_band = Table("income_band")
        t_income_band.columns.append(Column("ib_income_band_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_income_band.columns.append(Column("ib_lower_bound", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_income_band.columns.append(Column("ib_upper_bound", ColumnType(ColumnTypeEnum.INT), self.nullable))

        order_cols = ["ib_income_band_sk"]
        shard_cols = ["ib_income_band_sk"]
        t_income_band.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)
        return t_income_band

    def promotion(self):
        t_promotion = Table("promotion")
        t_promotion.columns.append(Column("p_promo_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_promotion.columns.append(Column("p_promo_id", ColumnType(ColumnTypeEnum.CHAR, [16]), self.nullable))
        t_promotion.columns.append(Column("p_start_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_promotion.columns.append(Column("p_end_date_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_promotion.columns.append(Column("p_item_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_promotion.columns.append(Column("p_cost", ColumnType(ColumnTypeEnum.DECIMAL, [15, 2]), self.nullable))
        t_promotion.columns.append(Column("p_response_target", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_promotion.columns.append(Column("p_promo_name", ColumnType(ColumnTypeEnum.CHAR, [50]), self.nullable))

        t_promotion.columns.append(Column("p_channel_dmail", ColumnType(ColumnTypeEnum.CHAR, [1]), self.nullable))
        t_promotion.columns.append(Column("p_channel_email", ColumnType(ColumnTypeEnum.CHAR, [1]), self.nullable))
        t_promotion.columns.append(Column("p_channel_catalog", ColumnType(ColumnTypeEnum.CHAR, [1]), self.nullable))
        t_promotion.columns.append(Column("p_channel_tv", ColumnType(ColumnTypeEnum.CHAR, [1]), self.nullable))

        t_promotion.columns.append(Column("p_channel_radio", ColumnType(ColumnTypeEnum.CHAR, [1]), self.nullable))
        t_promotion.columns.append(Column("p_channel_press", ColumnType(ColumnTypeEnum.CHAR, [1]), self.nullable))
        t_promotion.columns.append(Column("p_channel_event", ColumnType(ColumnTypeEnum.CHAR, [1]), self.nullable))
        t_promotion.columns.append(Column("p_channel_demo", ColumnType(ColumnTypeEnum.CHAR, [1]), self.nullable))

        t_promotion.columns.append(Column("p_channel_details", ColumnType(ColumnTypeEnum.VARCHAR, [100]), self.nullable))
        t_promotion.columns.append(Column("p_purpose", ColumnType(ColumnTypeEnum.CHAR, [15]), self.nullable))
        t_promotion.columns.append(Column("p_discount_active", ColumnType(ColumnTypeEnum.CHAR, [1]), self.nullable))

        order_cols = ["p_promo_sk"]
        shard_cols = ["p_promo_sk"]
        t_promotion.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)
        return t_promotion

    def reason(self):
        t_reason = Table("reason")
        t_reason.columns.append(Column("r_reason_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_reason.columns.append(Column("r_reason_id", ColumnType(ColumnTypeEnum.CHAR, [16]), self.nullable))
        t_reason.columns.append(Column("r_reason_desc", ColumnType(ColumnTypeEnum.CHAR, [100]), self.nullable))

        order_cols = ["r_reason_sk"]
        shard_cols = ["r_reason_sk"]
        t_reason.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)
        return t_reason

    def ship_mode(self):
        t_ship_mode = Table("ship_mode")
        t_ship_mode.columns.append(Column("sm_ship_mode_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_ship_mode.columns.append(Column("sm_ship_mode_id", ColumnType(ColumnTypeEnum.CHAR, [16]), self.nullable))
        t_ship_mode.columns.append(Column("sm_type", ColumnType(ColumnTypeEnum.CHAR, [30]), self.nullable))
        t_ship_mode.columns.append(Column("sm_code", ColumnType(ColumnTypeEnum.CHAR, [10]), self.nullable))

        t_ship_mode.columns.append(Column("sm_carrier", ColumnType(ColumnTypeEnum.CHAR, [20]), self.nullable))
        t_ship_mode.columns.append(Column("sm_contract", ColumnType(ColumnTypeEnum.CHAR, [20]), self.nullable))

        order_cols = ["sm_ship_mode_sk"]
        shard_cols = ["sm_ship_mode_sk"]
        t_ship_mode.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)
        return t_ship_mode


    def time_dim(self):
        t_time_dim = Table("time_dim")
        t_time_dim.columns.append(Column("t_time_sk", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_time_dim.columns.append(Column("t_time_id", ColumnType(ColumnTypeEnum.CHAR, [16]), self.nullable))
        t_time_dim.columns.append(Column("t_time", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_time_dim.columns.append(Column("t_hour", ColumnType(ColumnTypeEnum.INT), self.nullable))

        t_time_dim.columns.append(Column("t_minute", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_time_dim.columns.append(Column("t_second", ColumnType(ColumnTypeEnum.INT), self.nullable))
        t_time_dim.columns.append(Column("t_am_pm", ColumnType(ColumnTypeEnum.CHAR, [2]), self.nullable))
        t_time_dim.columns.append(Column("t_shift", ColumnType(ColumnTypeEnum.CHAR, [20]), self.nullable))
        t_time_dim.columns.append(Column("t_sub_shift", ColumnType(ColumnTypeEnum.CHAR, [20]), self.nullable))
        t_time_dim.columns.append(Column("t_meal_time", ColumnType(ColumnTypeEnum.CHAR, [20]), self.nullable))

        order_cols = ["t_time_sk"]
        shard_cols = ["t_time_sk"]
        t_time_dim.post_init(self.database, self.external_path, self.use_orders, order_cols, self.use_bucket, shard_cols)
        return t_time_dim

