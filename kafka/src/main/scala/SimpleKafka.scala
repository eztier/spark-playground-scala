package com.eztier.examples

import org.apache.spark._
import org.apache.spark.streaming._

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


class StreamsProcessor(brokers: String) {
  def process(): Unit = {
    // ...
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
    
    import StreamsProcessor.{master, ageFunc}
  
    val	spark	=	new	SparkSession().builder().appName("Simple Kafka application").master(master("no-parallelism")).getOrCreate()
    
    import spark.implicits._
    
    val brokers = "127.0.0.1:9092,172.17.0.1:9010,172.17.0.1:9096"
    
    val inputDf =
      spark.readStream
        .format("kafka.bootstrap.servers", brokers)
        .option("subscribe", "ADT")
        .load()
    
    /*
    val consoleOutput = inputDf.writeStream
      .outputMode("append")
      .format("console")
      .start()
      
    consoleOutput.awaitTermination()
    */
        
    val adtDf = inputDf.selectExpr("CAST(value AS STRING)")
    
    // PID segment
    val struct = new StructType()
    .add("firstName", DataTypes.StringType)
    .add("lastName", DataTypes.StringType)
    .add("birthDate", DataTypes.StringType)
    
    // TODO: convert string to Hapi Message, get PID.
    
    val pidDf = adtDf.select(from_json($"value", struct).as("pid"))
        
    val pidFlattenedDf = pidDf.selectExpr("pid.firstName", "pid.lastName", "pid.birthDate")
        
    val personDf = pidFlattenedDf.withColumn("birthDate", to_timestamp($"birthDate", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
        
    val ageUdf: UserDefinedFunction = udf(ageFunc, DataTypes.IntegerType)
    
    val processedDf = personDf.withColumn("age", ageUdf.apply($"birthDate"))
    
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
