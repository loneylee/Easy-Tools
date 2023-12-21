#!/usr/bin/bash

############################################################
# Load Clickhouse(alisa: ch) data from HDFS
############################################################

src_hive_database=$1
src_hive_table=$2
dest_ch_database=$3
dest_ch_table=$4
has_partition=$5 #true or false
partition=$6

############################################################
# Client for hive and CH.
# Maintained by operator.
############################################################
ch_host="localhost"
ch_port="8123"
ch_cluster_name="cluster1"

hive_client=(hive --database "${src_hive_database}" --silent)
clickhouse_client=(curl --silent "http://${ch_host}:${ch_port}/?" --data-binary @-)

ch_tmp_database="_load_data_tmp"
ch_tmp_table="${dest_ch_database}_${dest_ch_database}_$(date +%s%N)"

# src_hdfs_format options: Parquet
src_hdfs_format="Parquet"
hdfs_root_path="hdfs://namenode:8020/user/hive/warehouse"
############################################################
# System variables. Don't update.
############################################################
src_hdfs_path="${hdfs_root_path}/${src_hive_database}.db/${src_hive_table}/"
ch_on_cluster_sql="ON CLUSTER ${ch_cluster_name}"
ch_type=""
ch_create_table_columns=""
ch_create_table_partition_columns=""
partition_value=""
sql_partition_columns=""

if [[ "${has_partition}" == "true" ]]; then
  ch_create_table_partition_columns="dte"
  partition_value=",'${partition}'"
  src_hdfs_path="${src_hdfs_path}/${ch_create_table_partition_columns}=${partition}"
fi

############################################################
# Create CH table from hive schema.
############################################################
exists_table=$("${hive_client[@]}" -e "show tables like '${src_hive_table}';" | wc -l)
if [[ "${exists_table}" != "1" ]]; then
  echo "Table ${src_hive_database}.${src_hive_table} not exist in hive. Please check."
  exit 100
fi

function spark_column_type_to_ch() {
  spark_type=$1

  if [[ "$spark_type" == "boolean" ]]; then
    ch_type="Boolean"
  elif [[ "$spark_type" == "tinyint" ]]; then
    ch_type="Int8"
  elif [[ "$spark_type" == "smallint" ]]; then
    ch_type="Int16"
  elif [[ "$spark_type" == "int" ]]; then
    ch_type="Int32"
  elif [[ "$spark_type" == "bigint" ]]; then
    ch_type="Int64"
  elif [[ "$spark_type" == "float" ]]; then
    ch_type="Float32"
  elif [[ "$spark_type" == "double" ]]; then
    ch_type="Float64"
  elif [[ "$spark_type" == "decimal"* ]]; then
    p=$(echo "$spark_type" | awk -F "," '{print $1}' | awk -F "(" '{print $2}')
    s=$(echo "$spark_type" | awk -F "," '{print $2}' | awk -F ")" '{print $1}')
    ch_type="Decimal($p, $s)"
  elif [[ "$spark_type" == "varchar"* ]]; then
    ch_type="String"
  elif [[ "$spark_type" == "char"* ]]; then
    ch_type="String"
  elif [[ "$spark_type" == "string" ]]; then
    ch_type="String"
  elif [[ "$spark_type" == "timestamp" ]]; then
    ch_type="DateTime"
  elif [[ "$spark_type" == "date" ]]; then
    ch_type="Date"
  else
    echo "WARN: Unrecognized spark type ${spark_type}, use String instead."
    ch_type="String"
  fi
}

hive_table_columns=$("${hive_client[@]}" -e "desc ${src_hive_table};" | grep -v '^$' | awk -F " " '{print $1"|"$2}')

for line in $hive_table_columns; do
  if [[ "${line}" == "|" ]]; then
    continue
  fi

  if [[ "${line}" == "#"* ]]; then
    continue
  fi

  column_name=$(echo "$line" | awk -F "|" '{print $1}')
  column_type=$(echo "$line" | awk -F "|" '{print $2}')
  spark_column_type_to_ch "$column_type"

  if [[ "${column_name}" == "${ch_create_table_partition_columns}" ]]; then
    sql_partition_columns="${column_name} ${ch_type}"
    continue
  fi

  ch_create_table_columns="${ch_create_table_columns}${column_name} ${ch_type},"
done

ch_create_table_columns="${ch_create_table_columns%?}"

############################################################
# Create ch temp database and tmp table.
############################################################
order_by_columns="tuple()"
sql_partition_by_columns=""

if [ -n "$ch_create_table_partition_columns" ]; then
  order_by_columns="${ch_create_table_partition_columns}"
  sql_partition_by_columns="PARTITION BY ${ch_create_table_partition_columns}"
  sql_partition_columns=",${sql_partition_columns}"
fi

echo "CREATE DATABASE IF NOT EXISTS $dest_ch_database ${ch_on_cluster_sql}" | "${clickhouse_client[@]}"
echo "CREATE DATABASE IF NOT EXISTS $ch_tmp_database  ${ch_on_cluster_sql}" | "${clickhouse_client[@]}"

"${clickhouse_client[@]}" <<EOF
  CREATE TABLE ${ch_tmp_database}.${ch_tmp_table}_hdfs
  (${ch_create_table_columns})
  Engine=HDFS('${src_hdfs_path}/*', '$src_hdfs_format')
EOF

echo "Create ch HDFS engine table ${ch_tmp_database}.${ch_tmp_table}_hdfs success."

"${clickhouse_client[@]}" <<EOF
  CREATE TABLE ${ch_tmp_database}.${ch_tmp_table} ${ch_on_cluster_sql}
  (${ch_create_table_columns}${sql_partition_columns})
  Engine=MergeTree()
  ${sql_partition_by_columns}
  ORDER BY ${order_by_columns}
EOF

echo "Create ch table ${ch_tmp_database}.${ch_tmp_table} success."

"${clickhouse_client[@]}" <<EOF
  CREATE TABLE ${ch_tmp_database}.${ch_tmp_table}_distributed ${ch_on_cluster_sql}
  (${ch_create_table_columns}${sql_partition_columns})
  ENGINE=Distributed(${ch_cluster_name}, ${ch_tmp_database}, ${ch_tmp_table})
EOF

echo "Create ch table ${ch_tmp_database}.${ch_tmp_table}_distributed success."

"${clickhouse_client[@]}" <<EOF
  CREATE TABLE IF NOT EXISTS ${dest_ch_database}.${dest_ch_table} ${ch_on_cluster_sql}
  (${ch_create_table_columns}${sql_partition_columns})
  Engine=MergeTree()
  ${sql_partition_by_columns}
  ORDER BY ${order_by_columns}
EOF

echo "Create ch table ${dest_ch_database}.${dest_ch_table}  success."

"${clickhouse_client[@]}" <<EOF
  INSERT INTO ${ch_tmp_database}.${ch_tmp_table}_distributed
    SELECT * ${partition_value} FROM ${ch_tmp_database}.${ch_tmp_table}_hdfs
EOF

echo "Load ch tmp data success."

if [[ "${has_partition}" == "true" ]]; then
  partition_name=$(echo "SELECT DISTINCT partition FROM system.parts WHERE database='${ch_tmp_database}' and table='${ch_tmp_table}' AND active='1'" | "${clickhouse_client[@]}")
  "${clickhouse_client[@]}" <<EOF
    ALTER TABLE ${dest_ch_database}.${dest_ch_table} ${ch_on_cluster_sql} REPLACE PARTITION '${partition_name}' FROM ${ch_tmp_database}.${ch_tmp_table}
EOF
else
  echo "EXCHANGE TABLES ${dest_ch_database}.${dest_ch_table} AND ${ch_tmp_database}.${ch_tmp_table} ${ch_on_cluster_sql}" | "${clickhouse_client[@]}"
fi

echo "DROP TABLE ${ch_tmp_database}.${ch_tmp_table} ${ch_on_cluster_sql}" | "${clickhouse_client[@]}"
echo "DROP TABLE ${ch_tmp_database}.${ch_tmp_table}_distributed ${ch_on_cluster_sql}" | "${clickhouse_client[@]}"
echo "DROP TABLE ${ch_tmp_database}.${ch_tmp_table}_hdfs;" | "${clickhouse_client[@]}"
