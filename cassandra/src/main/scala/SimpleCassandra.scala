package com.eztier.examples

import java.util.Properties
import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra._

object SimpleCassandraApp {
  def getJdbcSource(sc: SparkContext) = {
    val url = "jdbc:sqlserver://localhost:1433;databaseName=test"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "admin")
    connectionProperties.put("password", "12345678")
    
    val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    connectionProperties.setProperty("Driver", driverClass)

    val table = "(select convert(int, convert(varbinary, left(convert(varchar(50), container, 1), 3) + '0', 1))/16 part_id, convert(varchar(50), container, 1) container, ty, convert(varchar(50), ref, 1) ref from dbo.kv) subset"

    val	sqlContext	=	new	SQLContext(sc)
    
    // Explain options
    // sqlContext.read.jdbc(url, where, connectionProperties).explain(true)
    // sqlContext.read.jdbc(url, where, connectionProperties).select("container", "ref").explain(true)
    
    // For writing back jdbc with 10 connections.
    // df.repartition(10).write.mode(SaveMode.Append).jdbc(jdbcUrl, "kv", connectionProperties)
    
    // val df = sqlContext.read.jdbc(url = url, table = table, numPartitions = 16, columnName = "part_id", lowerBound = 0, upperBound = 15, connectionProperties = connectionProperties)
    
    val df = sqlContext.read.jdbc(url = url, table = table, numPartitions = 16, columnName = "part_id", lowerBound = 0, upperBound = 15, connectionProperties = connectionProperties)
    
    df.explain(true)
    df
  }

  def main(args: Array[String]) {
    // https://stackoverflow.com/questions/36182828/not-able-to-change-authentication-in-spark-cassandra-connector
    // http://www.russellspitzer.com/2016/02/16/Multiple-Clusters-SparkSql-Cassandra/
    /*
      val csc = new CassandraSQLContext(SparkConnection._sc)
      csc.setConf(s"${cluster}:${keyspace}/spark.cassandra.connection.host", host)
      csc.setConf(s"${cluster}:${keyspace}/spark.cassandra.connection.port", port)
      csc.setConf(s"${cluster}:${keyspace}/spark.cassandra.auth.username", username)
      csc.setConf(s"${cluster}:${keyspace}/spark.cassandra.auth.password", password)
    */
    val conf: SparkConf = new SparkConf().setAppName("Simple Cassandra") // .setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    
    val df = getJdbcSource(sc)
    // display(df)
    
    val rows: RDD[Row] = df.rdd

    rows.saveToCassandra("test", "kv")

    val rdd = sc.cassandraTable("test", "kv")
    println("COUNT >> " + rdd.count)
  }

  def main2(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setAppName("Simple Cassandra") // .setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    /*
    // Manually set configuration.
    val	sqlContext	=	new	SQLContext(sc)
    val spark = sqlContext.sparkSession
    spark.setCassandraConf("default", "test", ReadConf.SplitSizeInMBParam.option(128))
    */

    // Loading and analyzing data from Cassandra
    val rdd = sc.cassandraTable("test", "kv")
    println(rdd.count)
    println(rdd.first)
    // println(rdd.map(_.getInt("value")).sum)

    // Saving data from RDD to Cassandra
    // Add two more rows to the table:

    val collection = sc.parallelize(Seq(("key1", "val1"), ("key1", "val2"), ("key1", "val3")))
    collection.saveToCassandra("test", "kv", SomeColumns("key", "value"))
  }
}

/*
CREATE KEYSPACE if not exists test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
CREATE TABLE test.kv(part_id int, container text, ty text, ref text, primary key((part_id, container, ty), ref)) with clustering order by (ref asc);
 
INSERT INTO test.kv(part_id, container, ty, ref) VALUES (1, 'key1', 'ty1', 'val1');
INSERT INTO test.kv(part_id, container, ty, ref) VALUES (1, 'key1', 'ty1', 'val2');
INSERT INTO test.kv(part_id, container, ty, ref) VALUES (1, 'key1', 'ty1', 'val3');

$SPARK_HOME/bin/spark-shell --conf spark.cassandra.connection.host=127.0.0.1 \
  --conf spark.cassandra.auth.username=cassandra --conf spark.cassandra.auth.password=cassandra

*/
