import os

import config
from commons.utils.SQLHelper import Table, Column, ColumnType, ColumnTypeEnum, Shard
from resources.datasets.dataset import DataSetBase

TABLES = ["sv_pf_thr_pty_ar_dd", "bi_ifar_org", "sv_10dmp_s_tag_dtal_idv_hqcktr"]


class EXT1(DataSetBase):
    def __init__(self, database_: str, nullable_: bool = False, use_decimal_: bool = False, use_bucket_: bool = True,
                 external_path_: str = "", use_orders_: bool = False, use_partition_: bool = False):
        super().__init__(use_bucket_, use_partition_)
        self.database = database_
        self.nullable = nullable_
        self.use_decimal = use_decimal_  # TODO
        self.use_orders = use_orders_
        self.tables = {}
        self.external_path = external_path_
        self.is_init = False

    def _init_table(self):
        for table_name in TABLES:
            self.tables[table_name] = self.__class__.__getattribute__(self, "__" + table_name + "__")()

        self.is_init = True

    def get_tables(self) -> dict:
        if not self.is_init:
            self._init_table()

        return self.tables

    def set_external_path(self, external_path_: str):
        if len(external_path_) != 0:
            self.external_path = external_path_

    def __sv_pf_thr_pty_ar_dd__(self):
        name = "sv_pf_thr_pty_ar_dd"
        table = Table(name, self.database)

        table.repartition = config.shards_repartition.get(name)[1]
        table.external_path = self.external_path + os.sep + name

        table.add_column(Column("cust_typ", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("sigd_chanl", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("arg_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("ivtr_ext_offl_id", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("ccy_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("arg_pro_msk2", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(
            Column("rmb_arg_bal_sean_avrg", ColumnType(ColumnTypeEnum.DECIMAL, [36, 4]), self.nullable))
        table.add_column(Column("prod_temt_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("cur_sean_act_days_tot", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.add_column(Column("rmb_pre_arg_bal", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.add_column(Column("dte", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("ogu_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("std_prod_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("dts_bus_ctg_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("arg_lif_cyc_sta_cod", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("rmb_mth_arg_bal_accm", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.add_column(Column("bkrg_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("pre_arg_bal", ColumnType(ColumnTypeEnum.DECIMAL, [22, 2]), self.nullable))
        table.add_column(Column("ivtr_ext_id_typ_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("std_ogu_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("annl_arg_bal_accm", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.add_column(Column("dpstr_acct_no", ColumnType(ColumnTypeEnum.DECIMAL, [20, 0]), self.nullable))
        table.add_column(Column("arg_nam", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("arg_disb_ctrl_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("cust_nam", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("rmb_arg_bal_mth_avrg", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.add_column(Column("arg_actg_clsf_cod", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("prod_grp", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("yest_arg_bal", ColumnType(ColumnTypeEnum.DECIMAL, [22, 2]), self.nullable))
        table.add_column(Column("arg_lcs_chgrsn_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("lif_cyc_sta_en_dat", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("vlu_dat", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("rem_1", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("date_arg_last", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("sean_arg_bal_accm", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.add_column(Column("coprt_side_bus_unit_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("std_ccy_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("prov_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("arg_bal_mth_avrg", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.add_column(Column("matu_dat", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("arg_bal_yer_avrg", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.add_column(Column("cust_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("rmb_arg_bal_yer_avrg", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.add_column(Column("coprt_side_nam", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("cap_acct_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("ar_sts_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(
            Column("rmb_sean_arg_bal_accm", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.add_column(Column("year_act_days_tot", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.add_column(Column("bus_unit_nam", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("arg_bal", ColumnType(ColumnTypeEnum.DECIMAL, [22, 2]), self.nullable))
        table.add_column(Column("bus_breed_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("arg_crt_dat", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("prod_line", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("prod_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("lcs_strt_dat", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("date_arg_pre", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("csh_ex_ind", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("coprt_side_usg_acct_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("rmb_arg_bal", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.add_column(Column("arg_bal_sean_avrg", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.add_column(Column("bnk_acct_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("arg_prop_cod", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("arg_pro_msk1", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("arg_sor_typ_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("rmb_yest_arg_bal", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.add_column(Column("mth_arg_bal_accm", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.add_column(Column("prod_cls_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(
            Column("rmb_annl_arg_bal_accm", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.add_column(Column("naty", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("mth_act_days_tot", ColumnType(ColumnTypeEnum.INT), self.nullable))

        if self.use_orders:
            table.order_cols = ["dte", "ogu_cd"]

        if self.use_bucket:
            table.order_cols = ["ogu_cd"]
            table.shard_cols = Shard(
                ["ogu_cd"],
                config.shards_repartition.get(name)[0],
                table.order_cols
            )

        if self.use_partition:
            table.partition_cols = ["dte"]

        return table

    def __bi_ifar_org__(self):
        name = "bi_ifar_org"
        table = Table(name, self.database)
        table.repartition = config.shards_repartition.get(name)[1]
        table.external_path = self.external_path + os.sep + name

        table.add_column(Column("orgprp", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_snam_2", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_sort_3", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.add_column(Column("org_no_0", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_snam_0", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_sort_1", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.add_column(Column("org_snam", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("cnlflg", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_snam_6", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_snam_4", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_nam_6", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_nam_4", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_nam_2", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_nam_0", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_no_7", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_sort", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_no_3", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_sort_6", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.add_column(Column("org_no_5", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_snam_3", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_sort_4", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.add_column(Column("org_snam_1", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_sort_2", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.add_column(Column("org_no_1", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_sort_0", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.add_column(Column("org_lvl", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.add_column(Column("org", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_snam_7", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_nam", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("cntflg", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("up_org_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_snam_5", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_nam_5", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_nam_3", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_nam_1", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_no_6", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_no_2", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_sort_7", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.add_column(Column("org_no_4", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("enddate", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org_sort_5", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.add_column(Column("org_nam_7", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("startdate", ColumnType(ColumnTypeEnum.STRING), self.nullable))

        if self.use_orders:
            table.order_cols = ["org", "startdate"]

        if self.use_bucket:
            table.order_cols = ["org", "startdate"]
            table.shard_cols = Shard(
                ["org"],
                config.shards_repartition.get(name)[0],
                table.order_cols
            )

        return table

    def __sv_10dmp_s_tag_dtal_idv_hqcktr__(self):
        name = "sv_10dmp_s_tag_dtal_idv_hqcktr"
        table = Table(name, self.database)
        table.repartition = config.shards_repartition.get(name)[1]
        table.external_path = self.external_path + os.sep + name

        table.add_column(Column("mgr_bnk_eoash", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("mgr_stfid", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("yjgs_bnk_eoazh", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_subbrh_short", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_fsbrh_order_num", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.add_column(Column("apshtr_last_relocation_date", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("bz2", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_fsbrh_index", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("mgrog_typ", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("yjgseoaorg_index", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_tag4", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_tag5", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("yjgsog_typ", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("yjgsog_arp_ind", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("aum_maver", ColumnType(ColumnTypeEnum.DECIMAL, [18, 2]), self.nullable))
        table.add_column(Column("bal4", ColumnType(ColumnTypeEnum.DECIMAL, [18, 2]), self.nullable))
        table.add_column(Column("apshtr_org_scbrh_short", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("is_gtgs", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("cpt_bnk_cod", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_fsbrh_short", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("bz3", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtrcod", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("mgr_bnk_eoaqs", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_tag6", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_bnk_eoa", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_subbrh_eoa_id", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("ecore_cust", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_scbrh_index", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("zjyt", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("mgr_bnk", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("is_ggjf", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("mgrog_seat_area2", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_scbrh_eoa_id", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_og_arp_ind", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("yjgs_stf", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("is_gjzkh", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("bal3", ColumnType(ColumnTypeEnum.DECIMAL, [18, 2]), self.nullable))
        table.add_column(Column("mgrog_seat_area", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("is_dfgz", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("is_zhcj_xy", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("yjgs_bnk_eoash", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_tag10", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("gc_brth", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_fsbrh_eoa_id", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_tag2", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshcptprdno", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_subbrh_index", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("xjzz_flag", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("is_yxkh", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("yjgsog_seat_area2", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("is_sfcg", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("is_yxkkh", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("bal2", ColumnType(ColumnTypeEnum.DECIMAL, [18, 2]), self.nullable))
        table.add_column(Column("mgrog_brch_property_right", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("inout_flag", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("cust_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtrdat", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_subbrh_order_num", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.add_column(Column("apshtr_org_short", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("is_zhcj_qt", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("aum_yaver", ColumnType(ColumnTypeEnum.DECIMAL, [18, 2]), self.nullable))
        table.add_column(Column("is_nhkq", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("yjgs_stfid", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("bz1", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("jbgy", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("is_xyjf", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_tag3", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_last_overall_reno_date", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshprdno", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtramt", ColumnType(ColumnTypeEnum.DECIMAL, [18, 2]), self.nullable))
        table.add_column(Column("apshtr_og_typ", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtrchl", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("jbgy_id", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("bal1", ColumnType(ColumnTypeEnum.DECIMAL, [18, 2]), self.nullable))
        table.add_column(Column("apshtrbk", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("yjgs_bnk", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("is_zhcj_sq", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("is_sbkyx", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_scbrh_order_num", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.add_column(Column("dte", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("cust_sex_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_tag9", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("yjgs_bnk_eoaqs", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("is_dzkh", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("zjly", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("age", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.add_column(Column("apshactno", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshjrnno", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_order_num", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.add_column(Column("apshtr_og_seat_area", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("mgr_bnk_eoa", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("mgr_stfnam", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("mgrlast_relocation_date", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_tag1", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("is_shkh", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("yjgslast_overall_reno_date", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("yjgs_bnk_eoa", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("bus_bus_typ_gj", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("bus_bus_typ", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("bz4", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("bus_typ", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_tag7", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_og_brch_property_right", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_og_seat_area2", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("mgr_bnk_eoazh", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("mgreoaorg_index", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("yjgsog_brch_typ", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("is_fd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("yjgsog_brch_property_right", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("is_vipkh", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_index", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("yjgsog_seat_area", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("is_zysc", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("is_zhcj_df", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("bz5", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("mgrog_arp_ind", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("cptbnk", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("cust_level", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("mgrog_brch_typ", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_og_brch_typ", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtr_org_tag8", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("mgrlast_overall_reno_date", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("bus_typ_gj", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("org", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("yjgslast_relocation_date", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("bal5", ColumnType(ColumnTypeEnum.DECIMAL, [18, 2]), self.nullable))
        table.add_column(Column("is_zhcj_zq", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.add_column(Column("apshtrcnt", ColumnType(ColumnTypeEnum.INT), self.nullable))

        if self.use_orders:
            table.order_cols = ["org", "dte"]

        if self.use_bucket:
            table.order_cols = ["org", "dte"]
            table.shard_cols = Shard(
                ["org"],
                config.shards_repartition.get(name)[0],
                table.order_cols
            )

        if self.use_partition:
            table.partition_cols = ["dte"]

        return table
