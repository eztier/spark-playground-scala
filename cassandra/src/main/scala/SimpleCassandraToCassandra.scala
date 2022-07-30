package com.eztier.examples

import org.apache.spark.sql.{SparkSession, Dataset, Row, DataFrame}
import org.apache.spark.sql.cassandra._
import scala.util.{Try, Success, Failure}
import java.io.{PrintWriter, StringWriter}

object SimpleCassandraToCassandraApp {

  val cassandraOptions = Map(
    "cluster" -> System.getenv("CASSANDRA_CLUSTER"), // "Datacenter1"
    "spark.cassandra.connection.host" -> System.getenv("CASSANDRA_HOST"), // "localhost"
    "spark.cassandra.connection.port" -> System.getenv("CASSANDRA_PORT"), // 9042
    "spark.cassandra.input.split.size_in_mb" -> "64",
    "spark.cassandra.input.consistency.level" -> "LOCAL_ONE"
  )

  def main3(args: Array[String]) = {
  
    val spark = SparkSession.builder.appName("Simple cassandra 2 cassandra application").getOrCreate()

    import spark.implicits._
    
    val df = spark
      .read
      .cassandraFormat(System.getenv("CASSANDRA_FROM"), System.getenv("CASSANDRA_KEYSPACE"))
      .options(cassandraOptions)
      .load()

    df
      .write
      .cassandraFormat(System.getenv("CASSANDRA_TO"), System.getenv("CASSANDRA_KEYSPACE"))
      .options(cassandraOptions)
      .save
  
    spark.stop()

    System.exit(0)
    
  }

}

/*
CASSANDRA_CLUSTER="Datacenter1" \
CASSANDRA_HOST="127.0.0.1" \
CASSANDRA_PORT=9042 \
CASSANDRA_KEYSPACE=ks \
CASSANDRA_FROM=table_a \
CASSANDRA_TO=table_b \
spark-submit ... \
  --conf spark.cassandra.auth.username=cassandra \
  --conf spark.cassandra.auth.password=cassandra
*/

