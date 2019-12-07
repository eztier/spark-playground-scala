package com.eztier.examples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, Row, SparkSession, SaveMode}
import cats.implicits._

import frameless._
import frameless.syntax._
import frameless.ml._
import frameless.ml.feature._
import frameless.ml.regression._
import org.apache.spark.ml.linalg.Vector

case class PatientVisitFromTo(patientPostalCode: Int, visitFacility: Int, patientAge: Double)

case class FacilityFeatures(patientPostalCode: Int, visitFacility: Int)

object SimpleFramelessApp {
  def main(args: Array[String]) {

    val appID: String = new java.util.Date().toString + math.floor(math.random * 10E4).toLong.toString

    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Simple frameless app")
      .set("spark.ui.enabled", "false")
      .set("spark.app.id", appID)

    implicit def session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    implicit def sc: SparkContext = session.sparkContext
    
    val trainingData = TypedDataset.create(Seq(
      PatientVisitFromTo(10001, 1, 43),
      PatientVisitFromTo(10010, 2, 76)
    ))

    val assembler = TypedVectorAssembler[FacilityFeatures]

    case class PatientVisitFromToWithFeatures(patientPostalCode: Int, visitFacility: Int, patientAge: Double, features: Vector)

    val trainingDataWithFeatures = assembler.transform(trainingData).as[PatientVisitFromToWithFeatures]

    // Train the model.
    case class RandomForestInputs(patientAge: Double, features: Vector)
    
    val rf = TypedRandomForestRegressor[RandomForestInputs]
    
    val model = rf.fit(trainingDataWithFeatures).run()

    // Prediction.
    val testData = TypedDataset.create(Seq(PatientVisitFromTo(10018, 2, 0)))
    
    val testDataWithFeatures = assembler.transform(testData).as[PatientVisitFromToWithFeatures]
    
    case class VisitFacilityPrediction(
      patientPostalCode: Int, 
      visitFacility: Int,
      patientAge: Double,
      features: Vector,
      predictedAge: Double
    )
    
    val predictions = model.transform(testDataWithFeatures).as[VisitFacilityPrediction]
    
    predictions.select(predictions.col('predictedAge)).collect.run()
    
  }
}
