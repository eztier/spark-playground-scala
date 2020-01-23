package com.eztier.examples

import org.apache.spark.sql.cassandra._

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf

object SimpleCassandra2SolrApp {
  val cassandraOptions = Map(
    "cluster" -> System.getenv("CAS_CLUSTER"), // "Datacenter1"
    "host" -> System.getenv("CAS_HOST"), // "localhost"
    "port" -> System.getenv("CAS_PORT") // 9042 
  )

  val solrOptions = Map(
    "zkhost" -> System.getenv("ZK_HOST"), // "localhost:2181",
    "collection" -> System.getenv("SOLR_COLLECTION"),
    "gen_uniq_key" -> "true", // Generate unique key if the 'id' field does not exist
    "commit_within" -> "1000" // Hard commit for testing
  )
  
  def main(args: Array[String]) {
    
    val spark = SparkSession.builder.appName("Simple cassandra 2 solr application").getOrCreate()
    
    val df = spark
      .read
      .cassandraFormat("document_extracted", "dwh")
      .options(ReadConf.SplitSizeInMBParam.option(32))
      .load()

    

  }
}
