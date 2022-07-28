package com.eztier.examples

import org.apache.spark.sql.{SparkSession, Dataset, Row, DataFrame}
import org.apache.spark.sql.cassandra._
import scala.util.{Try, Success, Failure}
import java.io.{PrintWriter, StringWriter}

import com.eztier.clickmock.entity.grantsmock.domain.types._

object SimpleCassandraCirceApp {

  case class DocumentData(
    rootId: String = ""
  )

  case class SnapshotData(
    rootId: String = "",
    current: String = ""
  )

  case class MergedData(
    rootId: String = "",
    grantsId: String = ""
  )

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
    import io.circe.generic.auto._, io.circe.syntax._, io.circe.parser._
    
    val ds = spark
      .read
      .cassandraFormat(System.getenv("CASSANDRA_FROM"), System.getenv("CASSANDRA_KEYSPACE"))
      .options(cassandraOptions)
      .load()
      .filter($"domain" === System.getenv("DOC_DOMAIN") and $"doc_year_created" > 0)
      .limit(10)
      .select(
        'root_id
      )
      .as[DocumentData]

    val ds2 = ds.map { row =>
    
      val rootId = row.rootId
      val snapshotDs = spark
        .read
        .cassandraFormat("ca_resource_snapshot", "dwh")
        .options(cassandraOptions)
        .load()
        .filter($"store" === System.getenv("DOC_DOMAIN") and $"type" === System.getenv("SNAPSHOT_TYPE") and $"purpose" === System.getenv("SNAPSHOT_PURPOSE") and $"id" === rootId)
        .limit(1)
        .select('id, 'current)
        .as[SnapshotData]
        
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

    // df2.explain
    // df2.show  

    ds2.write
      .option("header", "true")
      .csv("/tmp/out.csv")

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
CASSANDRA_CLUSTER="Datacenter1" \
CASSANDRA_HOST="127.0.0.1" \
CASSANDRA_PORT=9042 \
CASSANDRA_KEYSPACE=ks \
CASSANDRA_FROM=table_a \
CASSANDRA_TO=table_b \
DOC_DOMAIN=grants \
SNAPSHOT_TYPE=_FundingProposal  \
SNAPSHOT_PURPOSE=psoft-proposal-snapshot \
/spark/bin/spark-submit --master spark://localhost:7077  \
  --class com.eztier.examples.SimpleCassandraCirceApp \
  --conf spark.cassandra.auth.username=cassandra \
  --conf spark.cassandra.auth.password=cassandra \
  /root/apps/spark-playground-scala/jdbc/target/scala-2.12/simple-cassandra-circe-app-assembly-1.0.jar
*/

