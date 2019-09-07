name := "simple-cats-app"
version := "1.0"
scalaVersion := "2.11.12"

val sparkVersion = "2.4.3"
 
resolvers += Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins")
 
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

