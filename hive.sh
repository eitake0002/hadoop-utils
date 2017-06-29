#!/bin/sh

# Description:
#   Get partitions in table.
# Usage:
#   get-partitions <table_name>
function get-partitions()
{
    hive -e "show partitions $1"
}

# Description:
#   Get metadata in table.
# Usage:
#   desc-table <table_name>
# Example:
#   desc-table my_table
function desc-table()
{
    hive -e "desc formatted $1"
}

# Description:
#   Exec desc extended formatted.
# Usage:
#   show-extended-table <table_name>
# Example:
#   show-extended-table my_table
function show-extended-table()
{
    hive -e "show table extended like $1"
}

# Description:
#   Drop partition.
# Usage:
#   drop-partition <table_name> <partition_date>
# Example:
#   drop-partition my_table 2000-01-01
function drop-partition()
{
    hive -e "alter table $1 drop partition (ver='$2')"
}

# Description:
#   Show add partition command.
# Usage:
#   add-partition
function add-partition(){
    echo 'Syntax  : alter table <table_name> add partition <partition>'
    echo "Example : alter table dmktportal_userinfo_parq add partition(ver='2016-01-04', sub_ver='00-00-00', fname='none')"
}

# Description:
#   Load data from file on HDFS.
function load-data()
{
    hive -e 'LOAD DATA [LOCAL] INPATH "filepath" [OVERWRITE] INTO TABLE tablename [PARTITION (partcol1=val1, partcol2=val2 ...)]'
}

# Description:
#   Get cut partitions.
# Usage:
#   cut-partition <table_name> <field_number>
# Example:
#   cut-partition my_table 1
function cut-partition()
{
  table_name=$1
  field_num=$2
  hive -e "show partitions ${table_name}" | cut -d '/' -f ${field_num}
}

# Description:
#   Create hive table.
# Usage:
#   create-table <db_name> <table_name> <cols>
# Example:
#   create-table default my_table "col_1, col2"
function create-table()
{
  db_name=$1
  table_name=$2
  cols=$3
  
  hiveql="
    USE $db_name;

    CREATE TABLE ${table_name}_parq(
      ${cols}
    )
    PARTITIONED BY (ver string, sub_ver string, fname string)
    ROW FORMAT DELIMITED
      FIELDS TERMINATED BY '\t'
      LINES TERMINATED BY '\n'
    STORED AS PARQUET;
  "
  echo "$hiveql"
  hive -e "${hiveql}"
}
