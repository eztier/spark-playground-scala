name := "simple-cassandra-app"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.3"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
resolvers += Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins")
 
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.1"
libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "7.2.2.jre8"

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
    // case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
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
*/

