package com.eztier.examples

import java.util.Properties
import com.datastax.spark.connector._
import com.mongodb.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, Row, SparkSession, SaveMode}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.rdd.{ReadConf, CassandraTableScanRDD}
import com.datastax.driver.core.ConsistencyLevel
import org.apache.log4j.Logger
import java.time.ZoneOffset

object models {
  import java.time.{Instant, OffsetDateTime, ZoneId, ZoneOffset}
  import java.time.format.DateTimeFormatter
  
  case class Hl7Message(
    mrn: String,
    messageType: String,
    dateCreated: Long,
    dateTimezoneOffset: Int,
    dateLocal: String,
    raw: Seq[String]
  )

  val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
  
  private val now: Instant = Instant.now()
  val offsetDateTime = OffsetDateTime.ofInstant(now, ZoneId.systemDefault())
  private val dateString = offsetDateTime.format(dateTimeFormatter)
  private val zoneOffset: ZoneOffset = offsetDateTime.getOffset()
  
  val rawStr = Seq(
      "MSH|^~\\&|SENDING_APPLICATION|SENDING_FACILITY|RECEIVING_APPLICATION|RECEIVING_FACILITY|20190525134448||ADT^A08|03576920190525134448|P|2.3||||\r",
      "PID|1||035769^^^||MOUSE^MICKEY^J||19281118|M||W~B~I|123 Main St.^^Lake Buena Vista^FL^32830||(407)939-1289^^^^^^^^^theMainMouse@disney.com^|||||||||N~U|||||||||||||||||||\r"
    )

  val hl7Msg = Hl7Message(
    mrn = "035769",
    messageType = "ADT^A08",
    dateCreated = now.toEpochMilli(),
    dateTimezoneOffset = zoneOffset.getTotalSeconds(),
    dateLocal = dateString,
    raw = rawStr
  )

  val createTableCql = s"""
    CREATE TABLE ca_hl_7_stream (
        message_type text,
        day text,
        ts timeuuid,
        raw text,
        primary key((account, day, bucket), ts)
    ) WITH CLUSTERING ORDER BY (ts DESC) 
            AND COMPACTION = {'class': 'TimeWindowCompactionStrategy', 
                          'compaction_window_unit': 'DAYS', 
                          'compaction_window_size': 1};
  """
}

object SimpleMongoApp {
  /*
    Either use argument in spark shell (default is LOCAL_ONE)
    https://github.com/datastax/spark-cassandra-connector/blob/master/spark-cassandra-connector/src/main/scala/com/datastax/spark/connector/rdd/ReadConf.scala
    --conf spark.cassandra.input.consistency.level=ALL
  */
  def main(args: Array[String]) {
	  val spark = SparkSession.builder()
      .master("local")
      .appName("Simple Mongo")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/dump.hl7")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/dump.hl7")
      .getOrCreate()
      
    val sc: SparkContext = spark.sparkContext
  
    // Write
    import models._
    import org.bson.Document
    import com.mongodb.spark.config._
    import io.circe.generic.auto._, io.circe.syntax._
    import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}
  
    val docs = (1 to 10).flatMap { i =>
      (1 to 10).map { j =>
        val nextDt = offsetDateTime.minusDays(j).minusMonths(i)
        val nextDtStr = nextDt.format(dateTimeFormatter)
        
        val nextMsg = hl7Msg.copy(dateCreated = nextDt.toInstant().toEpochMilli(), dateLocal = nextDtStr)

        Document.parse(nextMsg.asJson.toString)  
      }
    }.toSeq

    val sparkDocuments = sc.parallelize(docs)

    // Uses WriteConfig
    // val writeConfig = WriteConfig(Map("collection" -> "hl7", "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))
    // MongoSpark.save(sparkDocuments, writeConfig)
    
    // Uses SparkConf
    // sparkDocuments.saveToMongoDB() 
    
    // Read
    import spark.implicits._

    // Filter
    val start = ZonedDateTime.of(2020, 5, 1, 0, 0, 0, 0, ZoneId.systemDefault())

    val from = start.toInstant().toEpochMilli()
    val to = start.plusMonths(1).toInstant().toEpochMilli()

    val rdd = MongoSpark.load(sc)
    // rdd.take(10).foreach(a => println(a.toJson))

    val df = rdd.toDF().select('mrn, 'messageType, 'dateCreated, 'dateTimezoneOffset, 'dateLocal, 'raw)

    val df2 = df.filter(df.col("dateCreated") >= from)
      .filter(df.col("dateCreated") < to)

    // Should be exactly 10.  
    println(df2.count())
      
    val ds = df2.as[Hl7Message]

    ds.take(10).foreach(a => println(a.asJson))

    spark.stop()
  }
}
