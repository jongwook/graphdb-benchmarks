sh cleanup.sh
JAVA_VER=$(java -version 2>&1 | sed 's/java version "\(.*\)\.\(.*\)\..*"/\1\2/; 1q')

if [ $JAVA_VER -lt 18 ]; then
  if command -v jabba > /dev/null 2>&1; then 
    jabba use 1.8; 
  else
    echo "Java >= 1.8 required"
    exit -1
  fi
fi
export JAVA_HOME

export MAVEN_OPTS="-Xmx16g"
rm -rf storage/*
mvn -Pbench test
