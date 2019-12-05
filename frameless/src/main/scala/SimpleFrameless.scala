import frameless._
import frameless.syntax._
import frameless.ml._
import frameless.ml.feature._
import frameless.ml.regression._
import org.apache.spark.ml.linalg.Vector

object SimpleFramelessApp {
  def main(args: Array[String]) {

    val trainingData = TypedDataset.create(Seq(
      PatientVisit("I", PersonLocation("IN1", "214", "1", "1", "S", ""))
    ))

  }
}
