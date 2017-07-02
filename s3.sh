
# About this file.
# 
# HDFS utilities(functions) to use AWS/S3.
# 
# First aws key is must be set.
#

# Set aws keys.
export s3_key_arg="-Dfs.s3n.awsAccessKeyId=$AWS_ACCESS_KEY_ID -Dfs.s3n.awsSecretAccessKey=$AWS_SECRET_ACCESS_KEY"
export s3_bucket_name=<bucket_name>
export s3_url="s3n://$s3_bucket_name"


# Description:
#   Restore file from Glacier to S3.
# Usage:
#   restore-single warehouse_dev/restore_test/test.log
function restore-glacier-file()
{
    aws s3api restore-object --bucket ${s3_bucket_name} --key $1 --restore-request Days=1
}

# Description:
#   Restore folder from Glacier to S3.
# Usage:
#   restore-foler <s3_folder_path>
# Example:
#   restore-folder warehouse_dev/my_restore_folder
function restore-glacier-folder()
{
    target_path="${s3_url}/$1"
    restore_days=$2
    s3cmd restore --recursive --restore-days=${restore_days} $target_path
}

# Description:
#   Restore folder with specifing table and partition.
# Usage:
#   restore-dir-date <table_name> <partition>
# Usage:
#   restore-dir-date my_table 2014-01-01
function restore-glacier-date()
{
    path="$s3_url/warehouse/$1/ver=$2"
    echo "Resotre -> $path"
    hadoop fs $s3_key_arg -ls -R $path | grep '^-' | awk '{print $6}' | \
    xargs -n 1 -I target_path aws s3api restore-object --bucket ${s3_bucket_name} --key target_path --restore-request Days=1
}

# Description:
#   Copy from s3 to HDFS.
# Usage:
#   restore-s3-to-hdfs <s3_path> <hdfs_path>
function restore-s3-to-hdfs()
{
    hadoop fs cp -R $1 $2 
}

# Description:
#   Get item in folder.
# Usage:
#   s3-ls /warehouse
function s3-ls()
{
    aws s3 ls $1
}

# Description:
#   Chech object if exists.
# Usage:
#   s3-exists /warehouse 
function s3-exists()
{
    path="$s3_url$1"
    hadoop fs $s3arg -test -e $path
    [ $? = 1 ] && echo 'Not Exists' || echo 'Exists'
}

# Description:
#   Remove item.
# Usage:
#   delete-folder warehouse_dev/test
function delete-folder
{
    echo $1
    echo "Are you sure you want to delete? Y/n"
    read answer
    if [ $answer = "Y" ]; then
        aws s3 rm $1 --recursive
    fi
}

# Description:
#   Remove folder.
# Usage:
#   s3-delete /warehouse
function remove-folder-hadoop
{
    echo $1
    echo "Are you sure you want to delete? Y/n"
    read answer
    if [ $answer = "Y" ]; then
        hadoop fs $s3_key_arg -rm -R $1
    fi
}

