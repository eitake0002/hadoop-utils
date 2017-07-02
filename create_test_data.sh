#!/bin/sh

#
# About this file.
# Create test text data, sequence talbe, and parquet table, and insert them mutually.
#

# Description:
#   Create text file testdata.
# Usage:
#   create-text-data-with-text-file <number_of_line> <text file name>
# Example:
#   create-text-data-with-text-file 100 test.csv
function create-test-data-with-text-file()
{
    line_num=$1
    file_name=$2

    : > ${file_name}

    i=1
    while [ $i -le ${line_num} ]; do 
      echo "$i,test" >> ${file_name};
      i=$(expr $i + 1);
    done
}

# Description:
#   Create textfile format table.
#   cols: 
#     col_1 int
#     col_2 string
# Usage:
#   create-text-file-format-table <database> <table_name>
# Example:
#   create-text-file-format-table sandbox my_test_table
function create_text_table()
{
  database=$1
  table=$1

  hiveql="
    USE ${database};
    CREATE TABLE ${table} (
      col_1 int,
      col_2 string
    )
    PARTITIONED BY (ver string, sub_ver string, fname string)
    ROW FORMAT DELIMITED
      FIELDS TERMINATED BY ','
      LINES TERMINATED BY '\n';
  "

  hive -e "${hiveql}"
}

# Description
#   Put data from text file into text file format table.
# Usage:
#   insert-text-data <database> <table> <input file>
# Example:
#   insert-text-data sandbox my_text_table test.csv
function insert_text_data()
{
  database=$1
  table_name=$2
  input_file=$3

  hiveql="
    USE ${database};
    LOAD DATA LOCAL INPATH '${input_file}' OVERWRITE INTO TABLE ${table_name}
    PARTITION (ver='2016-01-01', sub_ver='none', fname='none');
  "

  echo "${hiveql}"
  hive -e "${hiveql}"
}

# Description:
#   Create sequence file format table.
#   cols:
#     col_1 int
#     col_2 string
# Usage:
#   create-seq-format-table <database> <table_name>
# Example:
#   create-seq-format-table sandbox my_seq_table
function create-seq-format-table()
{
  database=$1
  table=$2

  hiveql="
    use ${database};
    CREATE TABLE ${table} (
      col_1 int,
      col_2 string
    )
    PARTITIONED BY (ver string, sub_ver string, fname string)
    ROW FORMAT DELIMITED
      FIELDS TERMINATED BY '\t'
      LINES TERMINATED BY '\n'
    STORED AS SEQUENCEFILE;
  "

  hive -e "${hiveql}"
}

# Description:
#   Insert data from text file format table to sequence file format table.
# Usage:
#   insert_seq_data <sequence file format table> <text file format table>
# Example:
#   insert_seq_data my_sequence_table my_text_table
function insert_seq_data()
{
  seq_table=$1
  source_table=$2
  ver=$3

  hiveql="
    use sandbox;
    INSERT OVERWRITE TABLE ${seq_table}
    PARTITION(ver='${ver}', sub_ver='none', fname='none')
    SELECT
      col_1,
      col_2
    FROM
      ${source_table}
    
  "

  echo "${hiveql}"
  hive -e "${hiveql}"
}

