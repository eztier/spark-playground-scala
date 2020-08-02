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

object models {
  case class Hl7Message(
    mrn: String,
    messageType: String,
    dateCreated: Long,
    dateTimezoneOffset: Int,
    dateLocal: String,
    raw: Seq[String]
  )
}

object SimpleMongoApp {

  val rawStr = Seq(
      "MSH|^~\\&|SENDING_APPLICATION|SENDING_FACILITY|RECEIVING_APPLICATION|RECEIVING_FACILITY|20190525134448||ADT^A08|03576920190525134448|P|2.3||||\r",
      "PID|1||035769^^^||MOUSE^MICKEY^J||19281118|M||W~B~I|123 Main St.^^Lake Buena Vista^FL^32830||(407)939-1289^^^^^^^^^theMainMouse@disney.com^|||||||||N~U|||||||||||||||||||\r"
    )

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
    import models.Hl7Message
    import java.time.{Instant, OffsetDateTime, ZoneId, ZoneOffset}
    import java.time.format.DateTimeFormatter
    import org.bson.Document
    import com.mongodb.spark.config._
    import io.circe.generic.auto._, io.circe.syntax._
	
    val writeConfig = WriteConfig(Map("collection" -> "hl7", "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))
    val sparkDocuments = sc.parallelize((1 to 10).map { i => 
      val now: Instant = Instant.now()
      val offsetDateTime = OffsetDateTime.ofInstant(now, ZoneId.systemDefault())
      val dateString = offsetDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))
      val zoneOffset: ZoneOffset = offsetDateTime.getOffset()

      val hl7 = Hl7Message(
        mrn = "035769",
        messageType = "ADT^A08",
        dateCreated = now.toEpochMilli(),
        dateTimezoneOffset = zoneOffset.getTotalSeconds(),
        dateLocal = dateString,
        raw = rawStr
      )
      Document.parse(hl7.asJson.toString)
      
    })

    MongoSpark.save(sparkDocuments, writeConfig)
	
    
    spark.stop()
  }
}
