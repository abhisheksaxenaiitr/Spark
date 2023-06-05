import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JSON2 extends App {
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

  // Define the JSON schema for parsing
  val jsonSchema = "struct<id:bigint, issignal:boolean, line:string, value:struct<cnt:bigint, codePage:string, dataBytesString:string, encodeCode:string, last:double, sum:bigint, text:string, type:string>>"

  // Parse the JSON column as a struct
  val parsedDF = df.withColumn("parsedData", from_json(col("jsonData"), lit(jsonSchema)))

  // Explode the parsed JSON column
  val explodedDF = parsedDF.select(col("id"), col("parsedData.*"))

  // Show the results
  explodedDF.show()
}
