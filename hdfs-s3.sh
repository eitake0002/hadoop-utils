
export s3_bucket_name=d2c-aurum-warehouse
export s3_url="s3n://$s3_bucket_name"
export s3_key_arg="-Dfs.s3n.awsAccessKeyId=$AWS_ACCESS_KEY_ID -Dfs.s3n.awsSecretAccessKey=$AWS_SECRET_ACCESS_KEY"

#---------------------------------------------------------------
# Glacier -> S3復元コマンド
#---------------------------------------------------------------

# Usage:
#   restore-single warehouse_dev/restore_test/a.log
# Description:
#   ファイルを指定してリストア
#   * キー名の頭にスラッシュはつけない。
function restore-glacier-file()
{
    aws s3api restore-object --bucket d2c-aurum-warehouse --key $1 --restore-request Days=1
}

# Usage:
#   restore-folder warehouse_dev/restore_test_s3cmd
# Description:
#   指定ディレクトリをGlacierからS3へリストア
function restore-glacier-folder()
{
    target_path="s3://d2c-aurum-warehouse/$1"
    s3cmd restore --recursive --restore-days=1 $target_path
}

# Usage:
#   restore-dir-date fkx_log_2014 2014-01-01
# Description:
#   フォルダと日付を指定してGlacierからS3へリストア
function restore-glacier-date()
{
    path="$s3_url/warehouse/$1/ver=$2"
    echo "Resotre -> $path"
    hadoop fs $s3_key_arg -ls -R $path | grep '^-' | awk '{print $6}' | \
    xargs -n 1 -I target_path aws s3api restore-object --bucket d2c-aurum-warehouse --key target_path --restore-request Days=1
}


#---------------------------------------------------------------
# S3 -> HFDSリストア
#---------------------------------------------------------------

# Usage:
#   restore-s3-to-hdfs s3n://d2c-aurum-warehouse/warehouse/fkx_log_2014 /user/hive/warehouse
# Description:
#   S3からHDFSへリストア
function restore-s3-to-hdfs()
{
    hadoop fs cp -R $1 $2 
}


#---------------------------------------------------------------
# S3オブジェクト確認コマンド
#---------------------------------------------------------------

# Usage:
#   s3-ls /warehouse
# Description:
#   フォルダ内一覧表示
function s3-ls()
{
    aws s3 ls $1
}

# Usage:
#   s3-exists /warehouse 
# Description:
#   オブジェクトの存在確認
function s3-exists()
{
    path="$s3_url$1"
    hadoop fs $s3arg -test -e $path
    [ $? = 1 ] && echo 'Not Exists' || echo 'Exists'
}


#---------------------------------------------------------------
# 削除コマンド
#---------------------------------------------------------------

# Usage:
#   delete-folder warehouse_dev/test
# Description:
#   AWS/CLIを使用した対象フォルダを削除
function delete-folder
{
    echo $1
    echo "Are you sure you want to delete? Y/n"
    read answer
    if [ $answer = "Y" ]; then
        aws s3 rm $1 --recursive
    fi
}

# Usage:
#   s3-delete /warehouse
# Description:
#   Hadoopコマンドを使用した対象フォルダの削除
#   hadoopコマンドで削除するためhdfs上の.Trashに入る。
function delete-folder-hadoop
{
    echo $1
    echo "Are you sure you want to delete? Y/n"
    read answer
    if [ $answer = "Y" ]; then
        hadoop fs $s3_key_arg -rm -R $1
    fi
}

