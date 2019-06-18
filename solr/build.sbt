name := "simple-jdbc-solr-app"
organization in ThisBuild := "com.eztier"
scalaVersion in ThisBuild := "2.11.12"

lazy val compilerOptions = Seq(
  "-unchecked",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-deprecation",
  "-encoding",
  "utf8",
  "-Ylog-classpath",
  "-Ypartial-unification"
)

lazy val commonSettings = Seq(
  version := "1.0",
  organization := "com.eztier",
  scalaVersion := "2.11.12",
  scalacOptions ++= compilerOptions,
  resolvers ++= Seq(
    Resolver.sonatypeRepo("public"),
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots"),
    Resolver.bintrayRepo("hseeberger", "maven"),
    Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins"),
    "Restlet Repository" at "http://maven.restlet.org"
  )
)

lazy val settings = commonSettings

lazy val global = project
  .in(file("."))
  .settings(
    name := "global"
    settings,
    assemblySettings,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.4.3" % "provided",
      "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided",
      "com.lucidworks.spark" % "spark-solr" % "3.6.0",
      "com.microsoft.sqlserver" % "mssql-jdbc" % "7.2.2.jre8"
    )
  )
  .aggregate(
  )
 
// resolvers += Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins")
// resolvers += "Restlet Repository" at "http://maven.restlet.org"
 
// libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3" % "provided"
// libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided"
// libraryDependencies += "com.lucidworks.spark" % "spark-solr" % "3.6.0"
// libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "7.2.2.jre8"


// http://queirozf.com/entries/creating-scala-fat-jars-for-spark-on-sbt-with-sbt-assembly-plugin#spark-2-deduplicate-different-file-contents-found-in-the-following
// https://translate.google.com/translate?hl=en&sl=zh-CN&u=https://blog.csdn.net/xiewenbo/article/details/53573440&prev=search
lazy val assemblySettings = Seq(
  assemblyJarName in assembly := s"${name.value}-${version.value}.jar",
  
  assemblyMergeStrategy in assembly := {
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      val strategy = oldStrategy(x)
      if (strategy == MergeStrategy.deduplicate) 
          MergeStrategy.first 
        else 
          strategy
  }, 

  /*
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
  */
  test in assembly := {}
)
