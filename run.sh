
JAVA_VER=$(java -version 2>&1 | sed 's/java version "\(.*\)\.\(.*\)\..*"/\1\2/; 1q')

if [ $JAVA_VER -le 18 ] && command -v jabba > /dev/null 2>&1; then jabba use 1.8; fi
export JAVA_HOME

export MAVEN_OPTS="-Xmx16g"
rm -rf storage/*
mvn -Pbench test
