package com.eztier.examples

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.cassandra._

object SimpleCassandra2SolrApp {
  val cassandraOptions = Map(
    "cluster" -> System.getenv("CASSANDRA_CLUSTER"), // "Datacenter1"
    "spark.cassandra.connection.host" -> System.getenv("CASSANDRA_HOST"), // "localhost"
    "spark.cassandra.connection.port" -> System.getenv("CASSANDRA_PORT"), // 9042
    "spark.cassandra.input.split.size_in_mb" -> "64",
    "spark.cassandra.input.consistency.level" -> "LOCAL_ONE"
  )

  val solrOptions = Map(
    "zkhost" -> System.getenv("ZK_HOST"), // "localhost:2181",
    "collection" -> System.getenv("SOLR_COLLECTION"),
    "gen_uniq_key" -> "true", // Generate unique key if the 'id' field does not exist
    "commit_within" -> "1000" // Hard commit for testing
  )
  
  def main(args: Array[String]) {
    import com.datastax.spark.connector.cql._
    
    val spark: SparkSession = SparkSession.builder.appName("Simple cassandra 2 solr application").getOrCreate()
    // Configure cassandra manually.
    spark.setCassandraConf(cassandraOptions("cluster"), CassandraConnectorConf.ConnectionHostParam.option(cassandraOptions("host")) ++ CassandraConnectorConf.ConnectionPortParam.option(cassandraOptions("port").toInt))

    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.expressions.UserDefinedFunction
    import spark.implicits._

    val mapToListFunc: Map[String, String] => List[String] = m =>
      m.toList.map(a => s"${a._1}:${a._2}")

    val mapToListUdf: UserDefinedFunction = udf(mapToListFunc, DataTypes.createArrayType(DataTypes.StringType))
    
    val df = spark.sql(s"""
      select * from dwh.ca_document_extracted limit 10
    """)

    val df2 = df.withColumn("metadatalist", mapToListUdf($"metadata"))
      .select(
        $"id",
        $"domain",
        $"root_type",
        $"root_id",
        $"root_owner",
        $"root_associates",
        $"root_company",
        $"root_status",
        $"root_display",
        $"root_display_long",
        $"doc_id",
        $"doc_other_id",
        $"doc_file_path",
        $"doc_object_path",
        $"doc_category",
        $"doc_name",
        $"doc_date_created",
        $"doc_year_created",
        $"content",
        $"metadatalist".alias("metadata")
      )

      df2.write.format("solr").options(solrOptions).mode(org.apache.spark.sql.SaveMode.Overwrite).save
  }  

  def main2(args: Array[String]) {
    import com.datastax.spark.connector._
    import com.datastax.spark.connector.cql._
    import org.apache.spark.SparkContext
    
    val spark: SparkSession = SparkSession.builder.appName("Simple cassandra 2 solr application").getOrCreate()
    // Configure cassandra manually.
    spark.setCassandraConf(cassandraOptions("cluster"), CassandraConnectorConf.ConnectionHostParam.option(cassandraOptions("host")) ++ CassandraConnectorConf.ConnectionPortParam.option(cassandraOptions("port").toInt))

    val sc: SparkContext = spark.sparkContext
    
    val rdd = sc.cassandraTable("ca_document_extracted", "dwh")
      .map { row =>
        val n = row.getMap[String, String]("metadata").toList.map(a => s"${a._1}:${a._2}")
      }
  }

  def main3(args: Array[String]) {
    
    val spark = SparkSession.builder.appName("Simple cassandra 2 solr application").getOrCreate()
    
    val df = spark
      .read
      .cassandraFormat("ca_document_extracted", "dwh")
      .options(cassandraOptions)
      .load()

    df.explain

    df.show

    df.write.format("solr").options(solrOptions).mode(org.apache.spark.sql.SaveMode.Overwrite).save

  }
}

/*
CASSANDRA_CLUSTER="Datacenter1" \
CASSANDRA_HOST="127.0.0.1" \
CASSANDRA_PORT=9042 \
ZK_HOST="127.0.0.1:2181" \
SOLR_COLLECTION="document_extracted" \
spark-submit ... \
  --conf 'spark.driver.extraJavaOptions=-Dbasicauth=solr:SolrRocks' \
  --conf 'spark.executor.extraJavaOptions=-Dbasicauth=solr:SolrRocks' \
  --conf spark.cassandra.auth.username=cassandra \
  --conf spark.cassandra.auth.password=cassandra
*/
