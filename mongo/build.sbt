name := "simple-mongo-app"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.4"

val CirceVersion = "0.12.0-M3"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
resolvers += Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins")
 
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

libraryDependencies ++= List(
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.1",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.2",
  "io.circe" %% "circe-core" % CirceVersion,
  "io.circe" %% "circe-generic" % CirceVersion,
  "io.circe" %% "circe-parser" % CirceVersion
)

lazy val assemblySettings = Seq(
  assemblyJarName in assembly := s"${name.value}-${version.value}.jar",
  
  assemblyMergeStrategy in assembly := {
    case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
    case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
    case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
    case PathList("com", "google", xs @ _*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
    case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
    case "about.html" => MergeStrategy.rename
    case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
    case "META-INF/mailcap" => MergeStrategy.last
    case "META-INF/mimetypes.default" => MergeStrategy.last
    case "plugin.properties" => MergeStrategy.last
    case "log4j.properties" => MergeStrategy.last
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },
  test in assembly := {}
)



/*
$SPARK_HOME/bin/spark-shell --packages datastax:spark-cassandra-connector:2.4.1-s_2.11
$SPARK_HOME/bin/spark-submit --packages datastax:spark-cassandra-connector:2.4.1-s_2.11

From spark-shell:

$SPARK_HOME/bin/spark-shell --conf spark.cassandra.connection.host=127.0.0.1 \
                            --packages datastax:spark-cassandra-connector:2.4.1-s_2.11
                            
$SPARK_HOME/bin/spark-shell --driver-java-options "-Djava.net.preferIPv4Stack=true"
				--conf "spark.mongodb.input.uri=mongodb://127.0.0.1/test.myCollection?readPreference=primaryPreferred" \
                --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/test.myCollection" \
                --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.2
*/

