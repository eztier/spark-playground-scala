package com.eztier.examples

import org.apache.spark.{SparkConf,	SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import java.util.Properties
import java.sql.Timestamp

case class ParticipantHist(
  id: Option[String],
  from_store: Option[String],
  _uid: Option[String],
  start_time: Option[Timestamp],
  mrn: Option[String],
  firstName: Option[String],
  lastName: Option[String],
  dob: Option[String],
  processState: Option[String],
  streetAddressLine: Option[String],
  city: Option[String],
  state: Option[String],
  postalCode: Option[String],
  context: Option[String],
  processed: Option[Int],
  dateProcessed: Option[Timestamp],
  response: Option[String]
)

object SimpleJdbc2SolrApp {
  def main2(args: Array[String]) {
    
    // https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
    val user = System.getenv("SQL_USER")
    val password = System.getenv("SQL_PASSWORD")
    val url = System.getenv("SQL_URL")
    val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    val query = System.getenv("SQL_QUERY") 
    val spark = SparkSession.builder.appName("Simple jdbc 2 solr application").getOrCreate()
    
    val df = spark.read
      .format("jdbc")
      .option("url", url)
      .option("driver",	driverClass)
      .option("query", query)
      .option("user", user)
      .option("password", password)
      // .option("partitionColumn", "id") // Cannot be used with query.
      // .option("lowerBound", 1)
      // .option("upperBound", 10000)
      .option("numPartitions", 10)
      .load()

    /*
    val	conf	=	new	SparkConf().setAppName("Simple jdbc 2 solr application")
    val	sc	=	new	SparkContext(conf)
    val	sqlContext	=	new	SQLContext(sc)
    val spark = sqlContext.sparkSession
    
    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)
    connectionProperties.setProperty("Driver", driverClass)
    
    // This import is needed to use the $-notation
    import spark.implicits._

    // query should be in this format: "(select top 10 * from epic.participant_hist) as subset"
    val df = sqlContext.read.jdbc(url, query, connectionProperties)    
    val ds = df.as[ParticipantHist]    
    */

    /*
    val ds1 = df.selectExpr("id", "start_time", "from_store", "'epic' as to_store", "_uid as study_id", "'enroll:participant' as purpose", "array(mrn) as related", 
      "if (instr(response, 'responsecode>') > 0, substr(instr(response, 'responsecode>'), 80), left(response, 100)) as response")
    // ds1.show()

    // For testing
    import scala.util.Random.alphanumeric

    val ds2 = df.take(5).map(a => s"${a.id}::${alphanumeric.take(4).mkString("")}")
    ds2.foreach(println(_))

    val ds3 = df.take(5).map(a => a.id.slice(5, 10))
    ds3.foreach(println(_))
    */

    // solr
    val options = Map(
      "zkhost" -> System.getenv("ZK_HOST"), // "localhost:2181",
      "collection" -> System.getenv("SOLR_COLLECTION"),
      "gen_uniq_key" -> "true", // Generate unique key if the 'id' field does not exist
      "commit_within" -> "1000" // Hard commit for testing
    )
    
    // basicauth
    // spark-submit ... --conf 'spark.driver.extraJavaOptions=-Dbasicauth=solr:SolrRocks' 
    //   --conf 'spark.executor.extraJavaOptions=-Dbasicauth=solr:SolrRocks' 

    df.write.format("solr").options(options).mode(org.apache.spark.sql.SaveMode.Overwrite).save
    
    spark.stop()
    
  }
}
