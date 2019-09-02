name := "simple-kafka-app"
version := "1.0"
scalaVersion := "2.11.12"

val sparkVersion = "2.4.3"
 
resolvers += Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins")
 
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
// http://apache-spark-developers-list.1001551.n3.nabble.com/UnusedStubClass-in-1-3-0-rc1-td10767.html
// The following exclude is the only way to eliminate the deduplicate merge issue with sbt-assembly.
libraryDependencies += ("org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion)
  .exclude("org.spark-project.spark", "unused") 
libraryDependencies += "org.apache.spark" %% "spark-streaming" %  sparkVersion  % "provided"
libraryDependencies += "ca.uhn.hapi" % "hapi-structures-v231" % "2.3"

// http://queirozf.com/entries/creating-scala-fat-jars-for-spark-on-sbt-with-sbt-assembly-plugin#spark-2-deduplicate-different-file-contents-found-in-the-following
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
