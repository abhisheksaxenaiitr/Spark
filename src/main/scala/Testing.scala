import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.types._

object Testing extends App {

 case class Person(name: String, age: Int, salary: Int)




  // Create a Spark session
  val spark = SparkSession.builder()
    .appName("Spark Case Class Example")
    .master("local[*]")
    .getOrCreate()

  // Define the schema using the case classes
  val schema = Encoders.product[Person].schema

  // Read JSON data from a file
  val jsonPath = """C:\Users\C10123B\JSON\sample\sample.json"""
  val df = spark.read.schema(schema).json(jsonPath)
  df.printSchema()
  df.show()
}