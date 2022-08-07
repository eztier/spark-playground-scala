package com.eztier.examples

import org.apache.spark.{SparkConf,	SparkContext}
import org.apache.spark.sql.{SparkSession, Dataset, Row, DataFrame, SaveMode}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra._
import scala.util.{Try, Success, Failure}
import java.io.{PrintWriter, StringWriter}
import java.util.Properties

object SimpleCassandraHiveApp {

  def main(args: Array[String]) = {
    
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
}
