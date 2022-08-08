package com.eztier.examples

import org.apache.spark.{SparkConf,	SparkContext}
import org.apache.spark.sql.{SparkSession, Dataset, Row, DataFrame, SaveMode}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra._
import scala.util.{Try, Success, Failure}
import java.io.{PrintWriter, StringWriter}
import java.util.Properties

object SimpleCassandraHiveApp extends Serializable {

  // Write to hadoop from cassandra.
  // https://hadoopsters.com/how-to-load-data-from-cassandra-into-hadoop-using-spark-5987277b385d 
  case class MyCassandraTable(user_id: String, `type`: Int, key: String, value: String)

  def writeToHadoop = {
    /**************************************************************************************
      * INITIATE SPARK SESSION
      * This session will be used to ingest data from a Cassandra cluster.
      *************************************************************************************/
    val spark = SparkSession
      .builder()
      .config("hive.merge.orcfile.stripe.level", "false")
      .appName("Cassandra Data Loader")
      .enableHiveSupport()
      .getOrCreate()

    // Implicits Allow us to Use .as[CaseClass]
    import spark.implicits._

    /**************************************************************************************
      * SETUP CASSANDRA CONNECTION
      * These settings determine which environment, keyspace and table to download.
      *************************************************************************************/
    val user = Map("spark.cassandra.auth.username" -> "some_username")
    val pwd = Map("spark.cassandra.auth.password" -> "some_password")

    // Setup an entrypoint into your cassandra cluster from spark
    val hosts = "cassandra001.yourcompany.com,cassandra002.yourcompany.com,cassandra003.yourcompany.com"
    val port = "9042"

    // Set Cassandra Connection Configuration in Spark Session
    spark.setCassandraConf(
      CassandraConnectorConf.KeepAliveMillisParam.option(1000) ++
        CassandraConnectorConf.ConnectionHostParam.option(hosts) ++
        CassandraConnectorConf.ReadTimeoutParam.option(240000) ++
        CassandraConnectorConf.ConnectionPortParam.option(port) ++
        user ++ pwd)

    // Imply which keyspace.table to consume from Cassandra
    val table = Map("keyspace" -> "some_keyspace", "table" -> "some_table_in_that_keyspace")

    /**************************************************************************************
      * CONSUME DATA FROM CASSANDRA
      * Use the connector, via the format() method, to pull the data and write it.
      *************************************************************************************/
    val data = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(table)
      .load()
      .as[MyCassandraTable]

    // write to hdfs
    data
      .write
      .option("orc.compress", "snappy")
      .mode(SaveMode.Overwrite)
      .orc("/some/location/in/hdfs/")
  }

  // https://stackoverflow.com/questions/28048277/connecting-sparksql-hiveserver-to-cassandra
  def connectSparkThriftToCassandra = {
    val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .getOrCreate()

    val cassandraTable = spark.sqlContext
      .read
      .cassandraFormat("mytable", "mykeyspace", pushdownEnable = true)
      .load()

    cassandraTable.createGlobalTempView("mytable")

    spark.sqlContext.setConf("hive.server2.thrift.port", "10000")
    HiveThriftServer2.startWithContext(spark.sqlContext)
    System.out.println("Server is running")
  }

  def writeToHive = {
    val spark = SparkSession.builder.appName("Simple cassandra jdbc application").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val rdd = sc.cassandraTable("mydb", "Employee")

    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "Employee", "keyspace" -> "mydb" ))
      .load()

    df.filter("time between 2017041801 and 2017041804")
      .write.mode("overwrite").saveAsTable("hivedb.employee")
  }

  def main(args: Array[String]) = {
    
    
  }
}
