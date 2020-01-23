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
    
    val spark = SparkSession.builder.appName("Simple cassandra 2 solr application").getOrCreate()
    
    // Configure cassandra manually.
    // spark.setCassandraConf(cassandraOptions("cluster"), CassandraConnectorConf.ConnectionHostParam.option(cassandraOptions("host")) ++ CassandraConnectorConf.ConnectionPortParam.option(cassandraOptions("port").toInt))

    val df = spark
      .read
      .cassandraFormat("document_extracted", "dwh")
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
