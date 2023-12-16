import os

import config
from commons.utils.SQLHelper import Table, Column, ColumnType, ColumnTypeEnum, Shard
from resources.datasets.dataset import DataSetBase

TABLES = ["sv_10dmp_s_tag_dtal_idv_hqcktr", "bi_ifar_org", "sv_10dmp_s_tag_dtal_idv_hqcktr"]


class EXT1(DataSetBase):
    def __init__(self, database_: str, nullable_: bool = False, use_decimal_: bool = False, use_bucket_: bool = True,
                 external_path_: str = "", use_orders_: bool = False):
        self.database = database_
        self.nullable = nullable_
        self.use_decimal = use_decimal_  # TODO
        self.use_bucket = use_bucket_
        self.use_orders = use_orders_
        self.tables = {}
        self.external_path = external_path_
        self.is_init = False

    def __sv_10dmp_s_tag_dtal_idv_hqcktr__(self):
        name = "sv_10dmp_s_tag_dtal_idv_hqcktr"
        table = Table(name, self.database)

        table.repartition = config.shards_repartition.get(name)[1]
        table.external_path = self.external_path + os.sep + name

        table.columns.append(Column("cust_typ", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("sigd_chanl", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("arg_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("ivtr_ext_offl_id", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("ccy_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("arg_pro_msk2", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(
            Column("rmb_arg_bal_sean_avrg", ColumnType(ColumnTypeEnum.DECIMAL, [36, 4]), self.nullable))
        table.columns.append(Column("prod_temt_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("cur_sean_act_days_tot", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.columns.append(Column("rmb_pre_arg_bal", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.columns.append(Column("dte", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("ogu_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("std_prod_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("dts_bus_ctg_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("arg_lif_cyc_sta_cod", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("rmb_mth_arg_bal_accm", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.columns.append(Column("bkrg_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("pre_arg_bal", ColumnType(ColumnTypeEnum.DECIMAL, [22, 2]), self.nullable))
        table.columns.append(Column("ivtr_ext_id_typ_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("std_ogu_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("annl_arg_bal_accm", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.columns.append(Column("dpstr_acct_no", ColumnType(ColumnTypeEnum.DECIMAL, [20, 0]), self.nullable))
        table.columns.append(Column("arg_nam", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("arg_disb_ctrl_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("cust_nam", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("rmb_arg_bal_mth_avrg", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.columns.append(Column("arg_actg_clsf_cod", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("prod_grp", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("yest_arg_bal", ColumnType(ColumnTypeEnum.DECIMAL, [22, 2]), self.nullable))
        table.columns.append(Column("arg_lcs_chgrsn_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("lif_cyc_sta_en_dat", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("vlu_dat", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("rem_1", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("date_arg_last", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("sean_arg_bal_accm", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.columns.append(Column("coprt_side_bus_unit_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("std_ccy_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("prov_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("arg_bal_mth_avrg", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.columns.append(Column("matu_dat", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("arg_bal_yer_avrg", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.columns.append(Column("cust_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("rmb_arg_bal_yer_avrg", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.columns.append(Column("coprt_side_nam", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("cap_acct_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("ar_sts_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(
            Column("rmb_sean_arg_bal_accm", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.columns.append(Column("year_act_days_tot", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.columns.append(Column("bus_unit_nam", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("arg_bal", ColumnType(ColumnTypeEnum.DECIMAL, [22, 2]), self.nullable))
        table.columns.append(Column("bus_breed_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("arg_crt_dat", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("prod_line", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("prod_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("lcs_strt_dat", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("date_arg_pre", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("csh_ex_ind", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("coprt_side_usg_acct_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("rmb_arg_bal", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.columns.append(Column("arg_bal_sean_avrg", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.columns.append(Column("bnk_acct_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("arg_prop_cod", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("arg_pro_msk1", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("arg_sor_typ_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("rmb_yest_arg_bal", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.columns.append(Column("mth_arg_bal_accm", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.columns.append(Column("prod_cls_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(
            Column("rmb_annl_arg_bal_accm", ColumnType(ColumnTypeEnum.DECIMAL, [26, 4]), self.nullable))
        table.columns.append(Column("naty", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("mth_act_days_tot", ColumnType(ColumnTypeEnum.INT), self.nullable))

        if self.use_orders:
            table.order_cols = ["dte"]

        if self.use_bucket:
            table.order_cols = ["dte"]
            table.shard_cols = Shard(
                ["dte"],
                config.shards_repartition.get(name)[0],
                table.order_cols
            )

        return table

    def __bi_ifar_org__(self):
        name = "bi_ifar_org"
        table = Table(name, self.database)
        table.repartition = config.shards_repartition.get(name)[1]
        table.external_path = self.external_path + os.sep + name

        table.columns.append(Column("orgprp", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_snam_2", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_sort_3", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.columns.append(Column("org_no_0", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_snam_0", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_sort_1", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.columns.append(Column("org_snam", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("cnlflg", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_snam_6", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_snam_4", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_nam_6", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_nam_4", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_nam_2", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_nam_0", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_no_7", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_sort", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_no_3", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_sort_6", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.columns.append(Column("org_no_5", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_snam_3", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_sort_4", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.columns.append(Column("org_snam_1", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_sort_2", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.columns.append(Column("org_no_1", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_sort_0", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.columns.append(Column("org_lvl", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.columns.append(Column("org", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_snam_7", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_nam", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("cntflg", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("up_org_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_snam_5", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_nam_5", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_nam_3", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_nam_1", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_no_6", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_no_2", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_sort_7", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.columns.append(Column("org_no_4", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("enddate", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org_sort_5", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.columns.append(Column("org_nam_7", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("startdate", ColumnType(ColumnTypeEnum.STRING), self.nullable))

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

        table.columns.append(Column("mgr_bnk_eoash", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("mgr_stfid", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("yjgs_bnk_eoazh", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_subbrh_short", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_fsbrh_order_num", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.columns.append(Column("apshtr_last_relocation_date", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("bz2", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_fsbrh_index", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("mgrog_typ", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("yjgseoaorg_index", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_tag4", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_tag5", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("yjgsog_typ", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("yjgsog_arp_ind", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("aum_maver", ColumnType(ColumnTypeEnum.DECIMAL, [18, 2]), self.nullable))
        table.columns.append(Column("bal4", ColumnType(ColumnTypeEnum.DECIMAL, [18, 2]), self.nullable))
        table.columns.append(Column("apshtr_org_scbrh_short", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("is_gtgs", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("cpt_bnk_cod", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_fsbrh_short", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("bz3", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtrcod", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("mgr_bnk_eoaqs", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_tag6", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_bnk_eoa", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_subbrh_eoa_id", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("ecore_cust", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_scbrh_index", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("ZJYT", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("mgr_bnk", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("is_ggjf", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("mgrog_seat_area2", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_scbrh_eoa_id", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_og_arp_ind", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("yjgs_stf", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("is_gjzkh", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("bal3", ColumnType(ColumnTypeEnum.DECIMAL, [18, 2]), self.nullable))
        table.columns.append(Column("mgrog_seat_area", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("is_dfgz", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("is_zhcj_xy", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("yjgs_bnk_eoash", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_tag10", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("gc_brth", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_fsbrh_eoa_id", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_tag2", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshcptprdno", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_subbrh_index", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("xjzz_flag", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("is_yxkh", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("yjgsog_seat_area2", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("is_sfcg", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("is_yxkkh", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("bal2", ColumnType(ColumnTypeEnum.DECIMAL, [18, 2]), self.nullable))
        table.columns.append(Column("mgrog_brch_property_right", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("inout_flag", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("cust_no", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtrdat", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_subbrh_order_num", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.columns.append(Column("apshtr_org_short", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("is_zhcj_qt", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("aum_yaver", ColumnType(ColumnTypeEnum.DECIMAL, [18, 2]), self.nullable))
        table.columns.append(Column("is_nhkq", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("yjgs_stfid", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("bz1", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("jbgy", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("is_xyjf", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_tag3", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_last_overall_reno_date", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshprdno", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtramt", ColumnType(ColumnTypeEnum.DECIMAL, [18, 2]), self.nullable))
        table.columns.append(Column("apshtr_og_typ", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtrchl", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("jbgy_id", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("bal1", ColumnType(ColumnTypeEnum.DECIMAL, [18, 2]), self.nullable))
        table.columns.append(Column("apshtrbk", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("yjgs_bnk", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("is_zhcj_sq", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("is_sbkyx", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_scbrh_order_num", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.columns.append(Column("dte", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("cust_sex_cd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_tag9", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("yjgs_bnk_eoaqs", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("is_dzkh", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("zjly", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("age", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.columns.append(Column("apshactno", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshjrnno", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_order_num", ColumnType(ColumnTypeEnum.INT), self.nullable))
        table.columns.append(Column("apshtr_og_seat_area", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("mgr_bnk_eoa", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("mgr_stfnam", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("mgrlast_relocation_date", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_tag1", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("is_shkh", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("yjgslast_overall_reno_date", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("yjgs_bnk_eoa", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("bus_bus_typ_gj", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("bus_bus_typ", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("bz4", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("bus_typ", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_tag7", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_og_brch_property_right", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_og_seat_area2", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("mgr_bnk_eoazh", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("mgreoaorg_index", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("yjgsog_brch_typ", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("is_fd", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("yjgsog_brch_property_right", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("is_vipkh", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_index", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("yjgsog_seat_area", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("is_zysc", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("is_zhcj_df", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("bz5", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("mgrog_arp_ind", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("cptbnk", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("cust_level", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("mgrog_brch_typ", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_og_brch_typ", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtr_org_tag8", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("mgrlast_overall_reno_date", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("bus_typ_gj", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("org", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("yjgslast_relocation_date", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("bal5", ColumnType(ColumnTypeEnum.DECIMAL, [18, 2]), self.nullable))
        table.columns.append(Column("is_zhcj_zq", ColumnType(ColumnTypeEnum.STRING), self.nullable))
        table.columns.append(Column("apshtrcnt", ColumnType(ColumnTypeEnum.INT), self.nullable))

        if self.use_orders:
            table.order_cols = ["org", "dte"]

        if self.use_bucket:
            table.order_cols = ["org", "dte"]
            table.shard_cols = Shard(
                ["org"],
                config.shards_repartition.get(name)[0],
                table.order_cols
            )

        return table
