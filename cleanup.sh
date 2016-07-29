export HBASE_CONF_DIR=hbase-conf
echo "disable 'benchmark-dev'" | hbase shell 
echo "drop 'benchmark-dev'" | hbase shell
rm -rf storage
