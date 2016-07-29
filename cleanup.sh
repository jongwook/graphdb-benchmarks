export JAVA_HOME=`/usr/libexec/java_home -v 1.$1`
export HBASE_CONF_DIR=hbase-conf
echo "disable 'benchmark-dev'" | hbase shell 
echo "drop 'benchmark-dev'" | hbase shell
echo "disable 'titan'" | hbase shell
echo "drop 'titan'" | hbase shell
rm -rf storage
