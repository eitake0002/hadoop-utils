function get-tables()
{
  hive -e 'show tables'
}
