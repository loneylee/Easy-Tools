


# 一些工具
* [查看clickhouse执行UI](#查看clickhouse执行ui)
* [生成Mergetree Bucket数据（Gluten读取）](#生成mergetreebucket数据)
* [通过csv造数](#通过csv造数)

## 查看clickhouse执行UI
clickhouse查询时需要带着`SETTINGS log_processors_profiles = 1`

执行命令
```shell
./profiles.sh clickhouse [arg1]
```

arg1支持2种方式传递
1. clickhouse查询的ID<br/>
   通过config.py配置clickhouse，程序自动访问clickhouse获取profiles。
   如:`./profiles.sh clickhouse [query_id]`

2. 手动导出profiles的csv文件<br/>
   运行sql
   ```sql
   select
    name, 
    event_time_microseconds, 
    id, 
    plan_step, 
    elapsed_us, 
    input_wait_elapsed_us, 
    output_wait_elapsed_us, 
    input_rows, 
    input_bytes, 
    output_rows, 
    output_bytes, 
    parent_ids 
    from system.processors_profile_log 
    where query_id=''
    order by event_time_microseconds asc format CSV
   ```
   将导出的csv保存为test.csv,传入绝对路径到脚本中，如`./profiles.sh clickhouse /tmp/test.csv`


## 生成MergetreeBucket数据
通过工具将Parquet生成为bucket mergetree数据，提供给[gluten](https://github.com/oap-project/gluten)读取




## 通过csv造数
通过csv造数据，横轴是列，纵轴是值可选范围

