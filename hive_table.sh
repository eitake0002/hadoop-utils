#!/bin/sh

# Description:
#   テストデータ用のテキストファイルを作成します。
# Usage:
#   create_test_data_file 行数 ファイル名
# Example:
#   create_test_data_file 100 test.csv
function create_test_data_file()
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
#   TextFileフォーマットのテーブルを作成します。
# Usage:
#   create_text_table <table_name>
# Example:
#   create_text_table my_text_table
function create_text_table()
{
  table_name=$1;

  hiveql="
    USE sandbox;
    CREATE TABLE ${table_name} (
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
#   TextFileフォーマットのテーブルにローカルファイルのデータを挿入します。
# Usage:
#   insert_text_data <table_name> <input_file>
# Example:
#   insert_text_data my_text_table test.csv
function insert_text_data()
{
  table_name=$1
  input_file=$2

  hiveql="
    USE sandbox;
    LOAD DATA LOCAL INPATH '${input_file}' OVERWRITE INTO TABLE ${table_name}
    PARTITION (ver='2016-01-01', sub_ver='none', fname='none');
  "

  echo "${hiveql}"
  hive -e "${hiveql}"
}

# Description:
#   SequenceFileフォーマットのテーブルを作成します。
# Usage:
#   create_seq_table <table_name>
# Example:
#   create_seq_table my_seq_table
function create_seq_table()
{
  table_name=$1;

  hiveql="
    use sandbox;
    CREATE TABLE ${table_name} (
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
#   TextFileフォーマットテーブルからSequenceFileフォーマットテーブルへデータを挿入します。
# Usage:
#   insert_seq_data <seq_table> <source_table>
# Example:
#   insert_seq_data my_seq_table my_text_table
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

# Description:
#   PARSUETフォーマットのテーブルを作成します。
# Usage:
#   create_parq_table <table_name>
# Example:
#   create_parq_table my_parq_table
function create_parq_table()
{
  table_name=$1

  hiveql="
    use sandbox;
    CREATE TABLE ${table_name} (
      col_1 int,
      col_2 string
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
#   SequenceFileフォーマットテーブルからPARQUETテーブルへデータを挿入します。
# Usage:
#   insert_parq_data <parq_table> <source_table>
# Example:
#   insert_parq_data my_parq_table my_seq_table
function insert_parq_data()
{
  parq_table=$1
  source_table=$2
  ver=$3

  hiveql="
    SET parquet.compression=GZIP;
    use sandbox;

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

