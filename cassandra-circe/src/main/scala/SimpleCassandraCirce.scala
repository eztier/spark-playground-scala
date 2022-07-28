package com.eztier.examples

import org.apache.spark.sql.{SparkSession, Dataset, Row, DataFrame}
import org.apache.spark.sql.cassandra._
import scala.util.{Try, Success, Failure}
import java.io.{PrintWriter, StringWriter}

import com.eztier.clickmock.entity.grantsmock.domain.types._

object SimpleCassandraCirceApp {

  val cassandraOptions = Map(
    "cluster" -> System.getenv("CASSANDRA_CLUSTER"), // "Datacenter1"
    "spark.cassandra.connection.host" -> System.getenv("CASSANDRA_HOST"), // "localhost"
    "spark.cassandra.connection.port" -> System.getenv("CASSANDRA_PORT"), // 9042
    "spark.cassandra.input.split.size_in_mb" -> "64",
    "spark.cassandra.input.consistency.level" -> "LOCAL_ONE"
  )

  def main(args: Array[String]) = {
  
    val spark = SparkSession.builder.appName("Simple cassandra with circe application").getOrCreate()

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
/spark/bin/spark-submit --master spark://localhost:7077  \
  --class com.eztier.examples.SimpleCassandraCirceApp \
  --conf spark.cassandra.auth.username=cassandra \
  --conf spark.cassandra.auth.password=cassandra \
  /root/apps/spark-playground-scala/jdbc/target/scala-2.12/simple-cassandra-circe-app-assembly-1.0.jar
*/

