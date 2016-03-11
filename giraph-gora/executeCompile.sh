#! /bin/bash
mvn clean compile package -DskipTests
cp conf/gora-cassandra-mapping.xml target/.
cp conf/gora-hbase-mapping.xml target/.
cp conf/gora.properties target/.
cp conf/zoo.cfg target/.

#/Users/keshann/Documents/workspace/workspaceGiraph/giraph/giraph-gora/conf/zoo.cfg

###################################
echo "[INFO] Initializing classpath"
###################################
export HADOOP_CLASSPATH=/Users/keshann/Documents/workspace/workspaceGiraph/giraph2/giraph-examples/target/giraph-examples-1.1.0-SNAPSHOT-for-hadoop-0.20.203.0-jar-with-dependencies.jar:/Users/keshann/Documents/workspace/workspaceGiraph/giraph2/giraph-gora/target/giraph-gora-1.1.0-SNAPSHOT-jar-with-dependencies.jar:/Users/keshann/Documents/workspace/workspaceGiraph/gora-trunk99/gora-hbase/lib/*


###################################
echo "[INFO] Cleaning up"
###################################
/Users/keshann/Documents/Apache/Hadoop/hadoop-0.20.203.0/bin/hadoop fs -rmr shortestPathsOutputGraph

###################################
echo "[INFO] Executing Hadoop"
###################################
#/Users/keshann/Documents/Apache/Hadoop/hadoop-0.20.203.0/bin/hadoop jar /Users/keshann/Documents/workspace/workspaceGiraph/giraph/giraph-examples/target/giraph-examples-1.1.0-SNAPSHOT-for-hadoop-0.20.203.0-jar-with-dependencies.jar org.apache.giraph.GiraphRunner -files /Users/keshann/Documents/workspace/workspaceGiraph/giraph/giraph-gora/target/gora-cassandra-mapping.xml,/Users/keshann/Documents/workspace/workspaceGiraph/giraph/giraph-gora/target/gora.properties,/Users/keshann/Documents/Apache/HBase/hbase-0.90.4/conf/hbase-site.xml,/Users/keshann/Documents/workspace/workspaceGiraph/giraph/giraph-gora/target/gora-hbase-mapping.xml -Dgiraph.metrics.enable=true -Dio.serializations=org.apache.hadoop.io.serializer.WritableSerialization,org.apache.hadoop.io.serializer.JavaSerialization -Dgiraph.gora.datastore.class=org.apache.gora.hbase.store.HBaseStore -Dgiraph.gora.key.class=java.lang.String -Dgiraph.gora.persistent.class=org.apache.giraph.io.gora.generated.GVertex -Dgiraph.gora.start.key=1 -Dgiraph.gora.end.key=101 -Dgiraph.gora.keys.factory.class=org.apache.giraph.io.gora.utils.DefaultKeyFactory -Dgiraph.gora.output.datastore.class=org.apache.gora.hbase.store.HBaseStore -Dgiraph.gora.output.key.class=java.lang.String -Dgiraph.gora.output.persistent.class=org.apache.giraph.io.gora.generated.GVertex -libjars /Users/keshann/Documents/workspace/workspaceGiraph/giraph/giraph-gora/target/giraph-gora-1.1.0-SNAPSHOT-jar-with-dependencies.jar,/Users/keshann/Documents/workspace/workspaceGiraph/gora-trunk99/gora-hbase/target/gora-hbase-0.3.jar,/Users/keshann/Documents/workspace/workspaceGiraph/gora-trunk99/gora-hbase/lib/hbase-0.90.4.jar  org.apache.giraph.examples.SimpleShortestPathsComputation -vif org.apache.giraph.io.gora.GoraGVertexVertexInputFormat -vof org.apache.giraph.io.gora.GoraGVertexVertexOutputFormat -op shortestPathsOutputGraph -w 1

/Users/keshann/Documents/Apache/Hadoop/hadoop-0.20.203.0/bin/hadoop jar /Users/keshann/Documents/workspace/workspaceGiraph/giraph2/giraph-examples/target/giraph-examples-1.1.0-SNAPSHOT-for-hadoop-0.20.203.0-jar-with-dependencies.jar org.apache.giraph.GiraphRunner -files /Users/keshann/Documents/workspace/workspaceGiraph/gora-trunk99/gora-hbase/lib/jackson-core-asl-1.6.9.jar,/Users/keshann/Documents/workspace/workspaceGiraph/gora-trunk99/gora-hbase/lib/jackson-jaxrs-1.5.5.jar,/Users/keshann/Documents/workspace/workspaceGiraph/gora-trunk99/gora-hbase/lib/jackson-mapper-asl-1.6.9.jar,/Users/keshann/Documents/workspace/workspaceGiraph/gora-trunk99/gora-hbase/lib/jackson-xc-1.5.5.jar,/Users/keshann/Documents/workspace/workspaceGiraph/giraph2/giraph-gora/target/gora-cassandra-mapping.xml,/Users/keshann/Documents/workspace/workspaceGiraph/giraph2/giraph-gora/target/gora.properties,/Users/keshann/Documents/Apache/HBase/hbase-0.90.4/conf/hbase-site.xml,/Users/keshann/Documents/workspace/workspaceGiraph/giraph2/giraph-gora/target/gora-hbase-mapping.xml -Dgiraph.metrics.enable=true -Dio.serializations=org.apache.hadoop.io.serializer.WritableSerialization,org.apache.hadoop.io.serializer.JavaSerialization -Dgiraph.gora.datastore.class=org.apache.gora.hbase.store.HBaseStore -Dgiraph.gora.key.class=java.lang.String -Dgiraph.gora.persistent.class=org.apache.giraph.io.gora.generated.GVertex -Dgiraph.gora.keys.factory.class=org.apache.giraph.io.gora.utils.DefaultKeyFactory -Dgiraph.gora.output.datastore.class=org.apache.gora.hbase.store.HBaseStore -Dgiraph.gora.output.key.class=java.lang.String -Dgiraph.gora.output.persistent.class=org.apache.giraph.io.gora.generated.GVertexResult -libjars /Users/keshann/Documents/workspace/workspaceGiraph/giraph2/giraph-gora/target/giraph-gora-1.1.0-SNAPSHOT-jar-with-dependencies.jar,/Users/keshann/Documents/workspace/workspaceGiraph/gora-trunk99/gora-hbase/target/gora-hbase-0.3.jar,/Users/keshann/Documents/workspace/workspaceGiraph/gora-trunk99/gora-hbase/lib/hbase-0.90.4.jar  org.apache.giraph.examples.SimpleShortestPathsComputation -vif org.apache.giraph.io.gora.GoraGVertexVertexInputFormat -vof org.apache.giraph.io.gora.GoraGVertexVertexOutputFormat -op shortestPathsOutputGraph -w 1

###################################
#echo "[INFO] Cleaning up"
###################################
#./Users/keshann/Documents/Apache/Hadoop/hadoop-0.20.203.0/bin/hadoop fs -rmr shortestPathsOutputGraph
