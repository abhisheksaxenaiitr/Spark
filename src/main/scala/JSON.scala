import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JSON extends App {
  // Create a SparkSession
  val spark = SparkSession.builder()
    .appName("NestedJSONExplodeExample")
    .master("local[*]")
    .getOrCreate()

  // Create a DataFrame with a nested JSON column
  val data = Seq(
    (1, "{\"id\": 3669, \"issignal\": true, \"line\": null, \"value\": {\"cnt\": null, \"codePage\": null, \"dataBytesString\": null, \"encodeCode\": null, \"last\": 190.05469470933627, \"sum\": null, \"text\": null, \"type\": null}}")
  )

  val df = spark.createDataFrame(data)
    .toDF("id", "jsonData")

  // Explode the original JSON column
  val explodedDF = df.select(col("id"), col("jsonData").as("value"))

  // Show the results
  explodedDF.show()
}
