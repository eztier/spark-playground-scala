package com.eztier.examples

import org.apache.spark.{SparkConf,	SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import java.util.Properties
import java.sql.Timestamp

import com.eztier.TextExtractor

object SimpleTikaApp {
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
    
  }
}
