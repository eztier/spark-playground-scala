package com.eztier.examples

import java.text.SimpleDateFormat
import java.util.Date
import java.time.Period;
import java.time.{LocalDate, LocalDateTime};
import java.sql.Timestamp

import org.apache.spark._
import org.apache.spark.streaming._

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction

import ca.uhn.hl7v2.model.Message
import ca.uhn.hl7v2.model.v231.segment.PID

import com.eztier.hl7mock.Hapi.parseMessage

class StreamsProcessor(brokers: String) {
  def process(): Unit = {
    // ...
  }
}

case class Patient(
  mrn: String = "",
  firstName: String = "",
  lastName: String = "",
  birthDate: Timestamp = new Timestamp(new Date().getTime()),
  age: Int = 0
)

object PatientSyntax {
  val sdf = new SimpleDateFormat("yyyy-MM-dd")

  implicit def HL7MessageToPatient(maybeMessage: Option[Message]) = {
    maybeMessage match {
      case Some(hpiMsg) => 
        val pid = hpiMsg.get("PID").asInstanceOf[PID]

        val dob = pid.getDateTimeOfBirth.getTimeOfAnEvent.getValueAsDate
        // val dt: Date = sdf.parse(dob)
        val ts = new java.sql.Timestamp(dob.getTime())
        
        val name = pid.getPatientName.map { a =>
          (a.getFamilyLastName.getFamilyName.getValueOrEmpty, a.getGivenName.getValueOrEmpty)
        }.headOption.getOrElse(("", ""))

        val mrn = pid.getPatientIdentifierList.map(_.getID.getValueOrEmpty).headOption.getOrElse("UNKNOWN")

        Patient(mrn, name._2, name._1, ts)
      case _ => Patient()
    }
  }
  
}

object StreamsProcessor {
  val master = Map(
    "no-parallelism" -> "local",
    "local-fixed-threads" -> "local[4]",
    "local-fixed-threads-with-max-failures" -> "local[4, 20]",
    "local-logical-cores" -> "local[*]",
    "local-logical-cores-with-max-failures" -> "local[*, 20]",
    "standalone-cluster" -> "spark://localhost:7077",
    "mesos-cluster" -> "mesos://zk://localhost:2181"
    )
    
  // Define a simple age function
  val ageFunc: java.sql.Timestamp => Int = birthDate => {
    val birthDateLocal = birthDate.toLocalDateTime().toLocalDate()
    val age = Period.between(birthDateLocal, LocalDate.now()).getYears()
    age
  }
    
  /*
  def main(args: Array[String]): Unit = {
    new StreamsProcessor("localhost:9092").process()
  }
  */
}

object SimpleKafkaApp {
  def main(args: Array[String]) {
    // Start the producer, wait for some records to be filled.
    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.Future
    val publisher = Future {
      Hl7KafkaProducer.produce
    }
    
    import StreamsProcessor.{master, ageFunc}
  
    val	spark	=	SparkSession
      .builder
      .appName("Simple Kafka application")
      .master(master("no-parallelism"))
      .getOrCreate()
    
    import spark.implicits._
    
    val brokers = "127.0.0.1:9092,172.17.0.1:9010,172.17.0.1:9096"
    
    val inputDf =
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", "ADT")
        .option("startingOffsets", "earliest")
        .load()
    
    /*
    val consoleOutput = inputDf.writeStream
      .outputMode("append")
      .format("console")
      .start()
      
    consoleOutput.awaitTermination()
    */
    
    inputDf.writeStream
      .format("console")
      .option("truncate", "false")
      .start()
      .awaitTermination()

    val adtDf = inputDf.selectExpr("CAST(value AS STRING)")
    
    /*
    // When the incoming kafka message is JSON format.
    val struct = new StructType()
    .add("firstName", DataTypes.StringType)
    .add("lastName", DataTypes.StringType)
    .add("birthDate", DataTypes.StringType)
    
    val pidDf = adtDf.select(from_json($"value", struct).as("pid"))
        
    val pidFlattenedDf = pidDf.selectExpr("pid.firstName", "pid.lastName", "pid.birthDate")
    
    val personDf = pidFlattenedDf.withColumn("birthDate", to_timestamp($"birthDate", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
        
    val ageUdf: UserDefinedFunction = udf(ageFunc, DataTypes.IntegerType)
    
    val processedDf = personDf.withColumn("age", ageUdf.apply($"birthDate"))
    */

    // Convert string to Hapi Message, get PID.
    val processedDf = adtDf.map {
      row => 
        val m = parseMessage(row.getAs[String]("value"))

        // Get PID etc...
        import PatientSyntax._

        val p: Patient = m
        p
    }.map { p =>
      p.copy(age = ageFunc(p.birthDate))
    }.toDF()
    
    // Key should be some dashboard dimension.
    val resDf = processedDf.select(
      concat($"firstName", lit(" "), $"lastName").as("key"),
      processedDf.col("age").cast(DataTypes.StringType).as("value"))
    
    /*
      mkdir -p /apps/kafka-docker/spark/checkpoints
      chmod -R 777 /apps/kafka-docker/spark/checkpoints
    */
    val kafkaOutput = resDf.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("topic", "ages")
      .option("checkpointLocation", "/apps/kafka-docker/spark/checkpoints")
      .start()

    kafkaOutput.awaitTermination()
    
    /*
    df.write
      .option("header", "true")
      .csv("/var/out.csv")
    */
    
  }
}
