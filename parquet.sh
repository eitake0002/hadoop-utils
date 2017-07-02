#!/bin/sh

# Description:
#   Create parquet file format table. 
#   cols:
#     col1 int
#     col2 string 
# Usage:
#   create-parq-file-format-table <database> <table>
# Example:
#   create-parq-file-format-table sandbox my_parq_table
function create_parq_table()
{
  database=$1
  table=$2

  hiveql="
    use ${database};
    CREATE TABLE ${table} (
      col1 int,
      col2 string
    )
    PARTITIONED BY (ver string, sub_ver string, fname string)
    ROW FORMAT DELIMITED
      FIELDS TERMINATED BY '\t'
      LINES TERMINATED BY '\n'
    STORED AS PARQUET;
  "

  hive -e "${hiveql}"
}


# Description:
#   Put data from sequence file format table into parquet file format table.
# Usage:
#   insert-parq-data <database> <parquet file format table> <sequence file format table>
# Example:
#   insert-parq-data sandbox my_parq_table my_seq_table
function insert_parq_data()
{
  database=$1
  parq_table=$2
  seq_table=$3
  ver=$4

  hiveql="
    SET parquet.compression=GZIP;
    use ${database};

    INSERT OVERWRITE TABLE ${parq_table}
    PARTITION(ver='${ver}', sub_ver='none', fname='none')
    SELECT
      col_1,
      col_2
    FROM
      ${source_table}
    WHERE ver = '${ver}'
  "

  hive -e "${hiveql}"
}

