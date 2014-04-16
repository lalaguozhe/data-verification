##data-verification

export HADOOP_CLASSPATH (include hive-site.xml ,it will connect to hive metastore service)
hadoop jar data-verification-1.0-SNAPSHOT-jar-with-dependencies.jar  com.dinaping.hive.DataVerification -t1 tableone -t2 tabletwo -dp diffpath -rn reducenumber

=================
