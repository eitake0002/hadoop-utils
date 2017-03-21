# このファイルについて
# 
# HDFSデータを操作するためのシェルスクリプト・ライブラリです。
# 必要ディレクトリのリスト化、検索、削除、S3/NASへのバックアップ等。
# 
# 【環境】
# interface03
# hdfsユーザー（シェルを読み込めば他ユーザーでも使用可能）
# 
# 【設定】
# 1. 当ファイル読み込み
# $ source hdfs.sh
# 2. .bash_profileにAWS/S3アクセスキー、シークレットアクセスキーを設定
# 


# Usage:
#   list-functions
# Description:
#   関数を一覧表示
function list-functions()
{
    cat /var/lib/hadoop-hdfs/scripts/warehouse_keeper/dev_tools/hdfs.sh | grep -B 4 function
}

#-------------------------------------------------------------------
# メタデータ取得
# tag: meta, metadata, meta-data
#-------------------------------------------------------------------

# Usage:
#   get-metadata /user/hive/warehouse/pki_raw_user pki_raw_user
# Description:
#   ディレクトリとテーブルを指定しメタデータを取得する。
#   メタデータ：所有ユーザー、データサイズ、最終日パーティション、最終アクセス時間、最終更新時間
function get-metadata()
{
    last_partition_date=`hadoop fs -ls $1 | grep -E 'ver=[0-9]{4}-[0-9]{2}-[0-9]{2}' | head -1 | awk '{print $8}'`

    hive_query="use default; show table extended like $2"
    extended_data=`hive -e "$hive_query" 2>/dev/null`

    own_user=`echo "$extended_data" | grep -e 'owner' | sed 's/owner://'`
    total_file_size=`echo "$extended_data" | grep -e 'totalFileSize' | sed 's/totalFileSize://'`
    last_access_date=`echo "$extended_data" | grep -e 'lastAccessTime' | grep -o -E '[0-9]{13}' | cut -c 1-10 | xargs -I val date -d "@val" | sed 's/ //g'`
    last_update_date=`echo "$extended_data" | grep -e 'lastUpdateTime' | grep -o -E '[0-9]{13}' | cut -c 1-10 | xargs -I val date -d "@val" | sed 's/ //g'`
    echo "$2,$own_user,$total_file_size,$last_partition_date,$last_access_date,$last_update_date"
}

# Usage:
#   get-metadata-list
# Description:
#   テーブルリストを読み込み、全てのテーブルのメタデータを取得する。
function get-metadata-list()
{
    while read line
    do
        get-metadata "/user/hive/warehouse/$line" $line
    done < default_table_list.csv
}

# Usage: 
#   create-hive-query-list
# Description:
#   テーブルリストを読み込み、全てのテーブルに対し"desc formatted table_name"を実行する。
function create-hive-query-list()
{
    hive_query=""
    while read line
    do
        hive_query=$hive_query"desc formatted $line;"
    done < default_table_list.csv
    hive -e "$hive_query"
}

# Usage:
#   get-show-extended-table-list
# Description:
#   全てのDefaultテーブルに"show table extended like tbl_name"を実行し表示する。
function get-show-extended-table-list()
{
    while read line
    do
        hive -e "show table extended like $line"
    done < default_table_list.csv
}


#-------------------------------------------------------------------
# 検索、一覧表示コマンド
# tag: search, list, ichiran, 
#-------------------------------------------------------------------

### サイズ条件
### tag: size, size condition, size-condition

# Usage:
#   list-item-size /user/hive/warehouse
# Description:
#   ディレクトリ・サイズ一覧表示
function list-item-size()
{
    hadoop fs -du -s "$1/*"
}

# Usage:
#   list-item-size-sort /user/hive/warehouse
# Description:
#   ディレクトリ・サイズ一覧をソートして表示
function list-item-size-sort()
{
    hadoop fs -du -s "$1/*" | sort -k 1 -n
}

# Usage:
#   list-table-size-sort /user/hive/warehouse 10
# Description:
#   指定ディレクトリ配下のテーブルをデータサイズ順に表示
function list-table-size-sort()
{
    hadoop fs -du -s "$1/*" | grep -v '.db' | sort -k 1 -n -r | head "-$2" | awk '{printf "%\047d ",$1; print $3}'
}

# Usage: 
#   list-zero-data /user/hive/wearehouse
# Description: 
#   空ディレクトリを一覧表示
function list-zero-data()
{
    hadoop fs -du -s "$1/*" | grep -e '^0'
}

# Usage:
#   count-item /user/hive/warehouse
# Description:
#   ディレクトリ数をカウント
function count-item()
{
    hadoop fs -ls "$1/*" | sed -e '1d' | wc -l
}

# Usage:
#   list-size-top-db /user/hive/warehouse 10
# Description:
#   容量の大きいデータベースを一覧取得
function list-size-top-db()
{
    hadoop fs -du -s "$1/*" | grep '.db' | sort -k 1 -n -r | head "-$2" | awk '{printf "%\047d ",$1; print $3}'
}

# Usage:
#   list-item-sec /user/hive/warehouse
# Description:
#   指定ディレクトリの日付を秒数に変換してリスト表示
function list-item-sec()
{
    hadoop fs -ls $1 | awk '{print $6, $8}' | xargs -n 2 sh -c 'echo `date -d $0 +%s` $0 $1'
}


### 日付（更新日）条件
### tag: date, date-condition, date condition

# Usage:
#   list-item-sort-last-update /user/hive/warehouse desc
# Description:
#   ディレクトリ一覧を最終更新日順にソートして表示
#   ＊デフォルトで昇順
function list-item-sort-last-update
{
    if test "$2" = 'desc'; then
        hadoop fs -ls $1 | sort -k 6,7 -r
    else
        hadoop fs -ls $1 | sort -k 6,7
    fi
}

# Usage:
#   list-item-date-ago /user/hive/warehouse 100 after
# Description:
#   指定更新日を軸に、過去、又は未来のディレクトリを一覧表示
# Args:
#   arg1: 対象ディレクトリ
#   arg2: 何日前か
#   arg3 : 過去か未来かを指定（無指定だと過去、afterを指定すると未来）
function list-item-date-ago()
{
    days_ago=`date -d "$2 days ago" "+%Y-%m-%d"`
    if test "$3" = 'after' ; then
        hadoop fs -ls $1 | sort -k 6 | awk --assign days_ago=$days_ago '$6 > days_ago {print $6, $8}'
    else
        hadoop fs -ls $1 | sort -k 6 | awk --assign days_ago=$days_ago '$6 < days_ago {print $6, $8}'
    fi
}

### パーティション条件
### tag: partition, partition-condition

# Usage:
#   get-last-date /user/hive/warehouse/fkx_log_2014
# Description:
#   最過去のパーティションを表示
function get-last-date()
{
    hadoop fs -ls $1 | sed 1d | head -1
}

# Usage:
#   list-last-date /user/hive/warehouse
# Description:
#   各ディレクトリの最終日パーティションを取得
function list-last-partition-date()
{
    hadoop fs -ls $1 | awk '{print $8}' | xargs -n 1 -I val sh -c "hadoop fs -ls val | sed 1d | head -1"
}

# Usage:
#   list-partition-date /user/hive/warehouse/fkx_log_2014 10 r
# Description:
#   日付を指定してディレクトリ配下のパーティション一覧を取得する。
#   Usageの例では/user/hive/warehouse/fkx_log_2014配下の１０日前より前のパーティションを取得する。
#   日付を使用しているパーティションのみ対応。
function list-partition-date()
{
    days=`date -d "$2 days ago" "+%Y-%m-%d"`
    if test $3 = 'feature' ; then
        # 指定日よりも未来を表示
        hadoop fs -ls $1 | awk '{print $8}' | grep -o -E '[0-9]{4}-[0-9]{2}-[0-9]{2}' | awk -v days=$days '$1 > days' | xargs -n 1 -I val echo "$1/ver=val"
    else
        # 指定日よりも過去を表示
        hadoop fs -ls $1 | awk '{print $8}' | grep -o -E '[0-9]{4}-[0-9]{2}-[0-9]{2}' | awk -v days=$days '$1 < days' | xargs -n 1 -I val echo "$1/ver=val"
    fi    
}

# Usage:
#   get-last-access-date sandbox test_evr017_purchase
# Description:
#   テーブルの最終サクセス日時を取得
function get-last-access-time()
{
    hive_command="use $1; show table extended like $2"
    hive -e "$hive_command" 2>/dev/null | grep -e 'lastAccessTime' -e 'lastUpdateTime' | grep -o -E '[0-9]{13}' | cut -c 1-10 | xargs -I val date -d "@val" | awk '{print $1, $2, $3, $4}'
}


### ユーザー、グループ条件
### tag: user, group, condition

# Usage:
#   list-filter-user /user/hive/warehouse hive
# Description:
#   指定ユーザのディレクトリを一覧表示
function list-filter-user()
{
    hadoop fs -ls $1 | awk --assign user_name=$2 '$3==user_name'
}

# Usage:
#   list-filter-user-size /user/hive/wearehouse hive
# Description:
#   指定ユーザーのディレクトリとサイズを一覧表示
function list-filter-user-size()
{
    hadoop fs -ls $1 | awk --assign user_name=$2 '$3==user_name {print $3, $8}' | xargs -n 2 sh -c 'echo -n "$0 "; hadoop fs -du -s -h $1'
}

# Usage:
#   list-filter-group /user/hive/warehouse hive
# Description:
#   グループでフィルタして表示
function list-filter-group()
{
    hadoop fs -ls $1 | awk --assign group_name=$2 '$4==group_name'
}

### データベース条件
### tag: database, db, condition, db-condition, database-condition

# Usage:
#   list-db-size 10
# Description:
#   データベース・ディレクトリをサイズの大きい順に一覧表示
function list-db-size()
{
    hadoop fs -du -s /user/hive/warehouse/* | grep '.db' | awk '{print $2, $3}' | sort -k1nr | head "-$1"
}


#-------------------------------------------------------------------
# 削除コマンド
# tag: remove, delete, condition
#-------------------------------------------------------------------

# Usage:
#   rm-file /user/hive/warehouse/test.txt
# Description:
#   指定ファイルを削除
function rm-file()
{
    echo $1
    echo "Are you sure you want to delete? Y/n"
    read answer
    if [ $answer = "Y" ]; then
        hadoop fs -rm $1
    fi
}

# Usage:
#   rm-dir /user/hive/warehouse/test_dir
# Description:
#   指定ディレクトリを削除
function rm-dir()
{
    echo $1
    echo "Are you sure you want to delete? Y/n"
    read answer
    if [ $answer = "Y" ]; then
        hadoop fs -rm -r $1
    fi
}

#-------------------------------------------------------------------
# S3コマンド
# tag: s3
#-------------------------------------------------------------------

# AWSキー設定
export s3_key_arg="-Dfs.s3n.awsAccessKeyId=$AWS_ACCESS_KEY_ID -Dfs.s3n.awsSecretAccessKey=$AWS_SECRET_ACCESS_KEY"

# Usage:
#   s3-ls /warehouse
# Description:
#   指定ディレクトリ配下を一覧表示
function s3-ls()
{
    hadoop fs $s3_key_arg -ls "s3n://d2c-aurum-warehouse$1"
}

# Usage:
#   s3-ls-recursive /warehouse
# Description:
#   指定ディレクトリ配下を再起一覧表示
function s3-ls-recursive()
{
    hadoop fs $s3_key_arg -ls -R "s3n://d2c-aurum-warehouse$1"
}

# Usage:
#   s3-upload-from-local /warehouse/acct_table s3n://d2c-aurum-warehouse/warehouse
# Description:
#   S3へフォルダアップロード
function s3-upload-from-local()
{
    hadoop fs $s3_key_arg -put $1 $2
}

#-------------------------------------------------------------------
# QNAP(NAS)コピーコマンド
# tag: qnap, nas, backup, copy, local
#-------------------------------------------------------------------

# Usage:
#   copy-to-nas /user/hive/warehouse/kashika_log/ver=2014-01-01 /mnt/nas_datastore/aurum.db/kashika_log
# Description:
#   対象ディレクトリをNASへコピー。
function copy-to-nas()
{
    nas_dir="/mnt/nas_datastore/aurum.db$2"
    hadoop fs -copyToLocal $1 "/mnt/nas_datastore/aurum.db$2"
    return $?
}

# Usage:
#   copy-to-nas-partition /user/hive/warehouse/kashika_log /mnt/nas/datastore/aurum.db/kashika_log 750
function copy-to-nas-partition()
{
    days=`date -d "$3 days ago" "+%Y-%m-%d"`
    copy_dirs=`hadoop fs -ls $1 | awk '{print $8}' | grep -o -E '[0-9]{4}-[0-9]{2}-[0-9]{2}' | awk -v days=$days '$1 < days' | xargs -n 1 -I val echo "$1/ver=val"`
    echo "$copy_dirs" | xargs -n 1 -I val hadoop fs -copyToLocal $1 $2
    return $?
}
