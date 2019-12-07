import frameless._
import frameless.syntax._
import frameless.ml._
import frameless.ml.feature._
import frameless.ml.regression._
import org.apache.spark.ml.linalg.Vector

case class PatientVisitFromTo(patientPostalCode: String, visitFacility: String, patientAge: Int)

case class FacilityFeatures(patientPostalCode: String, visitFacility: String)

object SimpleFramelessApp {
  def main(args: Array[String]) {

    val trainingData = TypedDataset.create(Seq(
      PatientVisitFromTo("10001", "A", 43),
      PatientVisitFromTo("10010", "B", 76)
    ))

    val assembler = TypedVectorAssembler[FacilityFeatures]

    case class PatientVisitFromToWithFeatures(patientPostalCode: String, visitFacility: String, features: Vector)

    val trainingDataWithFeatures = assembler.transform(trainingData).as[PatientVisitFromToWithFeatures]

    // Train the model.
    case class RandomForestInputs(patientPostalCode: String, visitFacility: String, features: Vector)
    
    val rf = TypedRandomForestRegressor[RandomForestInputs]
    
    val model = rf.fit(trainingDataWithFeatures).run()

    // Prediction.
    val testData = TypedDataset.create(Seq(PatientVisitFromTo("10018", "B", 0)))
    
    val testDataWithFeatures = assembler.transform(testData).as[PatientVisitFromToWithFeatures]
    
    case class VisitFacilityPrediction(
      postalCode: String, 
      facility: String,
      features: Vector,
      predictedAge: Int
    )
    
    val predictions = model.transform(testDataWithFeatures).as[VisitFacilityPrediction]
    
    predictions.select(predictions.col('predictedAge)).collect.run()
    
  }
}
