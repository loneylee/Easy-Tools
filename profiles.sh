#!/bin/bash

if [ $# == 0 ];then
    echo "参考示例"
    echo "./profiles.sh \$db \$query_id"
    echo ""
    echo "当前db只支持 clickhouse"
    echo ""
    echo "如果使用query_id，请到config.py填写ch的信息，以遍查询"
    echo ""
    echo "./profiles.sh clickhouse \$profiles.csv"
    echo "profiles.csv生成方式"
    echo "select \
    name, \
    event_time_microseconds, \
    id, \
    plan_step, \
    elapsed_us, \
    input_wait_elapsed_us, \
    output_wait_elapsed_us, \
    input_rows, \
    input_bytes, \
    output_rows, \
    output_bytes, \
    parent_ids \
from system.processors_profile_log \
where query_id='' \
order by event_time_microseconds asc format CSV"
    exit 10
fi

python3 profile.py --db "$1" --query-id "$2"