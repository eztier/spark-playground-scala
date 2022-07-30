package com.eztier.examples

import org.apache.spark.sql.{SparkSession, Dataset, Row, DataFrame}
import org.apache.spark.sql.cassandra._
import scala.util.{Try, Success, Failure}
import java.io.{PrintWriter, StringWriter}

import com.eztier.clickmock.entity.grantsmock.domain.types._

object SimpleCassandraCirceApp {

  case class DocumentData(
    root_id: String = "",
    root_type: String = ""
  )

  case class SnapshotData(
    id: String = "",
    current: String = ""
  )

  case class MergedData(
    rootId: String = "",
    grantsId: String = ""
  )

  val cassandraOptions = Map(
    "cluster" -> System.getenv("CASSANDRA_CLUSTER"), // "Datacenter1"
    // "table" -> System.getenv("CASSANDRA_FROM"),
    // "keyspace" -> System.getenv("CASSANDRA_KEYSPACE"),
    "spark.cassandra.connection.host" -> System.getenv("CASSANDRA_HOST"), // "localhost"
    "spark.cassandra.connection.port" -> System.getenv("CASSANDRA_PORT"), // 9042
    "spark.cassandra.input.split.size_in_mb" -> "64",
    "spark.cassandra.input.consistency.level" -> "LOCAL_ONE"
  )

  def main(args: Array[String]) = {
  
    val spark = SparkSession.builder.appName("Simple cassandra with circe application").getOrCreate()

    import spark.implicits._
    import io.circe.generic.auto._, io.circe.syntax._, io.circe.parser._
    
    val ds = spark
      .read
      // .format("org.apache.spark.sql.cassandra")
      .cassandraFormat(System.getenv("CASSANDRA_FROM"), System.getenv("CASSANDRA_KEYSPACE"))
      .options(cassandraOptions)
      .load()
      .filter($"domain" === System.getenv("DOC_DOMAIN") and $"doc_year_created" > 0)
      .limit(10)
      .select(
        'root_id,'root_type
      ).as[DocumentData]

    val ds2 = ds.map { row =>
    
      println(row)

      val rootId = row.root_id
      val rootType = row.root_type
      val snapshotPurpose = rootType match {
        case "_FundingProposal" => "psoft-proposal-snapshot"
        case "_FundingAward" => "psoft-award-snapshot"
        case _ => "Unknown"
      }
      val snapshotDs = spark
        .read
        .cassandraFormat("ca_resource_snapshot", "dwh")
        .options(cassandraOptions)
        .load()
        .filter($"environment" === System.getenv("SNAPSHOT_ENV") and $"store" === System.getenv("DOC_DOMAIN") and $"type" === rootType and $"purpose" === snapshotPurpose and $"id" === rootId)
        .limit(1)
        .select('id, 'current)
        .as[SnapshotData]
        
      snapshotDs.explain
      snapshotDs.show

      val defaultMergedData = MergedData( rootId = rootId)

      val mergedData: MergedData = if (snapshotDs.isEmpty) defaultMergedData
        else {
          val cur = snapshotDs.first.current
          val maybeParsed = parse(cur)
          maybeParsed match {
            case Right(a) => 
              System.getenv("SNAPSHOT_TYPE") match {
                case "_FundingProposal" => a.as[CkFundingProposalAggregate] match {
                  case Right(b) => defaultMergedData.copy(grantsId = b.identificationCm.get.identification.getOrElse(""))
                  case Left(e) => defaultMergedData
                }
                case "_FundingAward" => a.as[CkFundingAwardAggregate] match {
                  case Right(b) => 
                    if (b.fundingBucketAggregates.nonEmpty) defaultMergedData.copy(grantsId = b.fundingBucketAggregates.head.identificationCm.get.identification.getOrElse(""))
                    else defaultMergedData
                  case Left(e) => defaultMergedData
                }
                case _ => defaultMergedData
              }
            case Left(e) => defaultMergedData
          }
        }

      mergedData
    }.as[MergedData]
    
    ds2.printSchema

    ds2.explain
    ds2.show  

    /*
    ds2.write
      .option("header", "true")
      .csv("/tmp/out.csv")
    */

    /*  
    df2
      .write
      .cassandraFormat(System.getenv("CASSANDRA_TO"), System.getenv("CASSANDRA_KEYSPACE"))
      .options(cassandraOptions)
      .save
    */

    spark.stop()

    System.exit(0)
    
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
SNAPSHOT_ENV=production \
/spark/bin/spark-submit --master spark://localhost:7077  \
  --class com.eztier.examples.SimpleCassandraCirceApp \
  --conf spark.cassandra.auth.username=cassandra \
  --conf spark.cassandra.auth.password=cassandra \
  /root/apps/spark-playground-scala/cassandra-circe/target/scala-2.12/simple-cassandra-circe-app-assembly-1.0.jar
*/
