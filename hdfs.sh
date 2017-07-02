# About this file.
# 
# Functions to use HDFS(Hadoop Distributed File System).
# 
# How to use
# $ source hdfs.sh
# ... Execute function.
# 

# Description:
#   Get extended meta data.
# Usage:
#   get-metadata-extended <database> <table>
# Example:
#   get-metadata-exntended default my_table
function get-metadata-extended()
{

    $database=$1
    $table=$2
    hive -e "use ${database}; show table extended like ${table_name}"

}

# Description:
#   Get all metadata listed on file.
#   Beforehand, you need to put the file listed table names.
# Usage:
#   get-metadata-list
function get-metadata-list()
{
    while read line
    do
        get-metadata "/user/hive/warehouse/$line" $line
    done < table_list.csv
}

# Description:
#   Get all detailed metadata listed on file.
#   Beforehand, you need to put the file listed table names.
# Usage: 
#   create-hive-query-list
function create-hive-query-list()
{
    hive_query=""
    while read line
    do
        hive_query=$hive_query"desc formatted $line;"
    done < default_table_list.csv
    hive -e "$hive_query"
}

# Description:
#   Get all detailed metadata listed on file.
#   Beforehand, you need to put the file listed table names.
# Usage:
#   get-show-extended-table-list
function get-show-extended-table-list()
{
    while read line
    do
        hive -e "show table extended like $line"
    done < default_table_list.csv
}

# Description:
#   Get directory size list.
# Usage:
#   get-size-list <hdfs_path>
function get-size-list()
{
    hadoop fs -du -s "$1/*"
}

# Description:
#   Get directory list with sort.
# Usage:
#   get-size-list-sort <hdfs_path>
function list-item-size-sort()
{
    hadoop fs -du -s "$1/*" | sort -k 1 -n
}

# Description:
#   Get table list with sort in specified directory.
# Usage:
#   get-table-size-list-sort <hdfs_path> <top_number>
# Example:
#   get-table-size-list-sort /user/hive/warehouse 10
function get-table-size-list-sort()
{
    hadoop fs -du -s "$1/*" | grep -v '.db' | sort -k 1 -n -r | head "-$2" | awk '{printf "%\047d ",$1; print $3}'
}

# Description: 
#   Get empty directory list.
# Usage:
#   get-size-zero-data <hdfs_path>
# Example: 
#   get-size-zero-data /user/hive/wearehouse
function get-size-zero-data()
{
    hadoop fs -du -s "$1/*" | grep -e '^0'
}

# Description:
#   Get directory num(count).
# Usage:
#   count-item /user/hive/warehouse
function count-item()
{
    hadoop fs -ls "$1/*" | sed -e '1d' | wc -l
}

# Description:
#   Get big data list.
# Usage:
#   list-size-top-db /user/hive/warehouse 10
function list-size-top-db()
{
    hadoop fs -du -s "$1/*" | grep '.db' | sort -k 1 -n -r | head "-$2" | awk '{printf "%\047d ",$1; print $3}'
}

# Description:
#   Get directory list with date converted into timestamp.
# Usage:
#   list-item-sec /user/hive/warehouse
function list-item-sec()
{
    hadoop fs -ls $1 | awk '{print $6, $8}' | xargs -n 2 sh -c 'echo `date -d $0 +%s` $0 $1'
}

# Description:
#   Get directory list with update time sorted
# Usage:
#   list-item-sort-last-update /user/hive/warehouse desc
function list-item-sort-last-update
{
    if test "$2" = 'desc'; then
        hadoop fs -ls $1 | sort -k 6,7 -r
    else
        hadoop fs -ls $1 | sort -k 6,7
    fi
}

# Description:
#   Get directory axis with updated datetime.
# Usage:
#   list-item-date-ago /user/hive/warehouse 100 after
function list-item-date-ago()
{
    days_ago=`date -d "$2 days ago" "+%Y-%m-%d"`
    if test "$3" = 'after' ; then
        hadoop fs -ls $1 | sort -k 6 | awk --assign days_ago=$days_ago '$6 > days_ago {print $6, $8}'
    else
        hadoop fs -ls $1 | sort -k 6 | awk --assign days_ago=$days_ago '$6 < days_ago {print $6, $8}'
    fi
}

# Description:
#   Get oldest partition.
# Usage:
#   get-last-date <hdfs_path>
# Example:
#   get-last-date /user/hive/warehouse/my_table
function get-last-date()
{
    hadoop fs -ls $1 | sed 1d | head -1
}

# Description:
#   Get oldest directory in each tables.
# Usage:
#   list-last-date /user/hive/warehouse
function list-last-partition-date()
{
    hadoop fs -ls $1 | awk '{print $8}' | xargs -n 1 -I val sh -c "hadoop fs -ls val | sed 1d | head -1"
}

# Description:
#   Get directories in specified partition.
# Usage:
#   list-partition-date <hdfs_path> <date>
# Usage:
#   list-partition-date /user/hive/warehouse/my_table 10 r
function list-partition-date()
{
    days=`date -d "$2 days ago" "+%Y-%m-%d"`
    if test $3 = 'feature' ; then
        hadoop fs -ls $1 | awk '{print $8}' | grep -o -E '[0-9]{4}-[0-9]{2}-[0-9]{2}' | awk -v days=$days '$1 > days' | xargs -n 1 -I val echo "$1/ver=val"
    else
        hadoop fs -ls $1 | awk '{print $8}' | grep -o -E '[0-9]{4}-[0-9]{2}-[0-9]{2}' | awk -v days=$days '$1 < days' | xargs -n 1 -I val echo "$1/ver=val"
    fi    
}

# Description:
#   Get table access datetime.
# Usage:
#   get-last-access-date <db_name> <table_name>
# Example:
#   get-last-access-date default my_table
function get-last-access-time()
{
    hive_command="use $1; show table extended like $2"
    hive -e "$hive_command" 2>/dev/null | grep -e 'lastAccessTime' -e 'lastUpdateTime' | grep -o -E '[0-9]{13}' | cut -c 1-10 | xargs -I val date -d "@val" | awk '{print $1, $2, $3, $4}'
}

# Description:
#   Get specifiled user's table list.
# Usage:
#   list-filter-user <hdfs_path> <username>
# Example:
#   list-filter-user /user/hive/warehouse hive
function list-filter-user()
{
    hadoop fs -ls $1 | awk --assign user_name=$2 '$3==user_name'
}

# Description:
#   Get specified user's directory and size list.
# Usage:
#   list-filter-user-size /user/hive/wearehouse hive
function list-filter-user-size()
{
    hadoop fs -ls $1 | awk --assign user_name=$2 '$3==user_name {print $3, $8}' | xargs -n 2 sh -c 'echo -n "$0 "; hadoop fs -du -s -h $1'
}

# Description:
#   Filtering with group.
# Usage:
#   list-filter-group <hdfs_path> <group_name>
# Example:
#   list-filter-group /user/hive/warehouse hive
function list-filter-group()
{
    hadoop fs -ls $1 | awk --assign group_name=$2 '$4==group_name'
}

# Description:
#   Get list of database with size sort.
# Usage:
#   list-db-size <top_number>
# Example:
#   list-db-size 10
function list-db-size()
{
    hadoop fs -du -s /user/hive/warehouse/* | grep '.db' | awk '{print $2, $3}' | sort -k1nr | head "-$1"
}

# Description:
#   Remove file.
# Usage:
#   rm-file /user/hive/warehouse/test.txt
function rm-file()
{
    echo $1
    echo "Are you sure you want to delete? Y/n"
    read answer
    if [ $answer = "Y" ]; then
        hadoop fs -rm $1
    fi
}

# Description:
#   Remove directory.
# Usage:
#   rm-dir /user/hive/warehouse/test_dir
function rm-dir()
{
    echo $1
    echo "Are you sure you want to delete? Y/n"
    read answer
    if [ $answer = "Y" ]; then
        hadoop fs -rm -r $1
    fi
}

