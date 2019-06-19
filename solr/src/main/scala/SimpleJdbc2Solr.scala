package com.eztier.examples

import org.apache.spark.{SparkConf,	SparkContext}
import org.apache.spark.sql.SQLContext
import java.util.Properties
import java.sql.Timestamp

// For testing
import scala.util.Random.alphanumeric

case class ParticipantHist
(
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
  def main(args: Array[String]) {
    
    val	conf	=	new	SparkConf().setAppName("Simple jdbc 2 solr application")
    val	sc	=	new	SparkContext(conf)
    val	sqlContext	=	new	SQLContext(sc)
    val spark = sqlContext.sparkSession
    
    val url = "jdbc:sqlserver://localhost:1433;databaseName=test"

    val connectionProperties = new Properties()
    connectionProperties.put("user", "admin")
    connectionProperties.put("password", "12345678")
    
    val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    connectionProperties.setProperty("Driver", driverClass)
    
    val where = "(select top 10 * from epic.participant_hist) as subset"

    // This import is needed to use the $-notation
    import spark.implicits._

    val df = sqlContext.read.jdbc(url, where, connectionProperties)
    
    val ds = df.as[ParticipantHist]
    
    val ds1 = ds.selectExpr("id", "start_time", "from_store", "'epic' as to_store", "_uid as study_id", "'enroll:participant' as purpose", "array(mrn) as related", "response")
    
    val ds2 = ds.take(5).map(a => s"${a.id}::${alphanumeric.take(4).mkString("")}")
    ds2.foreach(println(_))

    val ds3 = ds.take(5).map(a => a.id.slice(5, 10))
    ds3.foreach(println(_))

    // val ds1 = ds.selectExpr("id", "start_time", "from_store", "'epic' as to_store", "_uid as study_id", "'enroll:participant' as purpose", "mrn as related", "response")
    
    // ds1.show()
    
    // solr
    val options = Map(
      "zkhost" -> "localhost:2181",
      "collection" -> "interface_logging",
      "gen_uniq_key" -> "true", // Generate unique key if the 'id' field does not exist
      "commit_within" -> "1000" // Hard commit for testing
    )
    
    ds1.write.format("solr").options(options).mode(org.apache.spark.sql.SaveMode.Overwrite).save
    
  }
}
