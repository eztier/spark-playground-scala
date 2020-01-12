package com.eztier.examples

import org.apache.spark.{SparkConf,	SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import java.util.Properties
import java.sql.Timestamp

import com.eztier.TextExtractor

object SimpleTikaApp {

  // https://tika.apache.org/1.8/examples.html
  def tikaFunc (a: (String, PortableDataStream)) = {

    val file: File = new File(a._1.drop(5))
    val parser: AutoDetectParser = new AutoDetectParser()
    val stream: InputStream = new FileInputStream(file)
    val handler: WriteOutContentHandler = new WriteOutContentHandler(-1)
    // val handler: BodyContentHandler = new BodyContentHandler()
    val metadata: Metadata = new Metadata()
    val context: ParseContext = new ParseContext()

    parser.parse(stream, handler, metadata, context)

    stream.close

    println(handler.toString())
  }

  def main(args: Array[String]) {

    // Setup spark context.
    val appID: String = new java.util.Date().toString + math.floor(math.random * 10E4).toLong.toString

    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Simple tika app")
      .set("spark.ui.enabled", "false")
      .set("spark.app.id", appID)

    implicit def session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    implicit def sc: SparkContext = session.sparkContext

    // Files must be hadoop compatible.
    val filesPath = "/home/user/documents/*"
    val fileData = sc.binaryFiles(filesPath)

    fileData
      .foreach(x => tikaFunc(x))
    
  }
}
