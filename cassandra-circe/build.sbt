name := "simple-cassandra-circe-app"

version := "1.0"

scalaVersion := "2.12.12"

val sparkVersion = "3.1.3"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
resolvers += Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins")
 
Global / resolvers ++= Seq(
  DefaultMavenRepository,
  Resolver.sonatypeRepo("public")
)

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

val CirceVersion = "0.12.1"
val CirceGenericExVersion = "0.12.2"
val SparkCassandraConnectorVersion = "3.1.0"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % SparkCassandraConnectorVersion
libraryDependencies += "io.circe" %% "circe-core" % CirceVersion
libraryDependencies += "io.circe" %% "circe-generic" % CirceVersion
libraryDependencies += "io.circe" %% "circe-parser" % CirceVersion
libraryDependencies += "io.circe" %% "circe-generic-extras" % CirceGenericExVersion

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
