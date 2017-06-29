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
