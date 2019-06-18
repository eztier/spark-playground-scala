name := "Simple Project"

version := "1.0"

scalaVersion := "2.11.12"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
libraryDependencies += "datastax" % "spark-cassandra-connector" % "2.4.1-s_2.11"

/*
$SPARK_HOME/bin/spark-shell --packages datastax:spark-cassandra-connector:2.4.1-s_2.11
$SPARK_HOME/bin/spark-submit --packages datastax:spark-cassandra-connector:2.4.1-s_2.11

From spark-shell:

$SPARK_HOME/bin/spark-shell --conf spark.cassandra.connection.host=127.0.0.1 \
                            --packages datastax:spark-cassandra-connector:2.4.1-s_2.11
*/

