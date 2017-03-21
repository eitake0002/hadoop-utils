
function global:int03
{
    cd ~/desktop
    ./interface03.ttl
}

function global:int03-send-file($file_name)
{
  scp $file_name hdfs@interface03:/var/lib/hadoop-hdfs/
}

function global:int03-get-from($file_name)
{
  scp hdfs@interface03:/var/lib/hadoop-hdfs/$file_name ./
}
