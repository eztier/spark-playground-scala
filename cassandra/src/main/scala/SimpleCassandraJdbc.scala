package com.eztier.examples

import org.apache.spark.{SparkConf,	SparkContext}
import org.apache.spark.sql.{SparkSession, Dataset, Row, DataFrame, SaveMode}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra._
import scala.util.{Try, Success, Failure}
import java.io.{PrintWriter, StringWriter}
import java.util.Properties

object SimpleCassandraJdbcApp {

  case class GrantsInfo(
    proposal_id: String,
    identification: String,
    pi_kerberos_id: String,
    pi_name: String,
    proposal_title: String
  )

  case class DocumentData(
    root_id: String,
    doc_year_created: Int,
    content: String
  )

  val cassandraOptions = Map(
    "cluster" -> System.getenv("CASSANDRA_CLUSTER"), // "Datacenter1"
    "spark.cassandra.connection.host" -> System.getenv("CASSANDRA_HOST"), // "localhost"
    "spark.cassandra.connection.port" -> System.getenv("CASSANDRA_PORT"), // 9042
    "spark.cassandra.input.split.size_in_mb" -> "64",
    "spark.cassandra.input.consistency.level" -> "LOCAL_ONE"
  )

  def main(args: Array[String]) = {
    
    val spark = SparkSession.builder.appName("Simple cassandra jdbc application").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val	sqlContext	=	new	SQLContext(sc)
    
    val url = System.getenv("SQL_URL")

    val connectionProperties = new Properties()
    connectionProperties.put("user", System.getenv("SQL_USER"))
    connectionProperties.put("password", System.getenv("SQL_PASSWORD"))
    
    val driverClass = System.getenv("SQL_DRIVER")
    connectionProperties.setProperty("Driver", driverClass)

    val where = System.getenv("SQL_QUERY")

    // This import is needed to use the $-notation
    import spark.implicits._

    val df = sqlContext.read.jdbc(url, where, connectionProperties)

    df.printSchema()
    
    val df2 = spark
      .read
      // .format("org.apache.spark.sql.cassandra")
      .cassandraFormat(System.getenv("CASSANDRA_FROM"), System.getenv("CASSANDRA_KEYSPACE"))
      .options(cassandraOptions)
      .load()
      .filter($"domain" === System.getenv("DOC_DOMAIN") and $"root_type" === System.getenv("DOC_ROOT_TYPE") and $"doc_year_created" > 0)
      .limit(10)
      .select(
        'root_id,'doc_year_created,'content
      )

    df2.printSchema()

    val ds = df.as[GrantsInfo]
    val ds2 = df2.as[DocumentData]

    ds.createOrReplaceTempView("info")
    ds2.createOrReplaceTempView("docs")  

    val joinDF2 = spark.sql("select e.*, d.content from info e INNER JOIN docs d ON e.proposal_id == d.root_id")
    joinDF2.show(false)

    joinDF2
      .write
      .option("header", true)
      // .option("compression","gzip")
      .mode(SaveMode.Overwrite)
      .csv(System.getenv("CSV_OUTPUT"))
    
    // spark.stop()

    // System.exit(0)
  
  }

}
  
/*
CASSANDRA_CLUSTER="Test Cluster" \
CASSANDRA_HOST="192.168.1.23" \
CASSANDRA_PORT=9042 \
CASSANDRA_KEYSPACE=dwh \
CASSANDRA_FROM=ca_document_extracted \
CASSANDRA_TO=table_b \
DOC_DOMAIN=grants \
DOC_ROOT_TYPE=_FundingProposal \
SNAPSHOT_ENV=production \
SQL_USER=admin \
SQL_PASSWORD=12345678 \
SQL_URL='jdbc:sqlserver://192.168.1.23:1433;databaseName=test' \
SQL_DRIVER="com.microsoft.sqlserver.jdbc.SQLServerDriver" \
SQL_QUERY="(select top 10 * from grants_info) as subset" \
CSV_OUTPUT="/tmp/extracted" \
/spark/bin/spark-submit --master spark://localhost:7077  \
  --class com.eztier.examples.SimpleCassandraJdbcApp \
  --conf spark.cassandra.auth.username=cassandra \
  --conf spark.cassandra.auth.password=cassandra \
  /root/apps/spark-playground-scala/cassandra/target/scala-2.11/simple-cassandra-app-assembly-1.0.jar
*/

