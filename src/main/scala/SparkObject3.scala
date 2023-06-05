import org.apache.spark.ml.feature.Imputer
import org.apache.spark.sql.functions.{col, concat, expr, lit, when}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}


object SparkObject3 extends App {
  val spark = SparkSession.builder()
    .appName("MissingValueImputationExample")
    .master("local")
    .getOrCreate()

  val data = Seq(
    (1, 10.0),
    (2, 20.0),
    (3, Double.NaN),
    (4, 40.0),
    (5, Double.NaN)
  )

  val dataFrame = spark.createDataFrame(data).toDF("id", "value")
  dataFrame.show()

  val imputer = new Imputer()
    .setInputCols(Array("value"))
    .setOutputCols(Array("imputed_value"))

  val imputerModel = imputer.fit(dataFrame)
  val imputedData = imputerModel.transform(dataFrame)

  imputedData.show()



}
