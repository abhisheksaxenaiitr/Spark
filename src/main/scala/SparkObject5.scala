import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object SparkObject5 extends App {

  case class Passenger(
                        PassengerId: Int,
                        Survived: Int,
                        Pclass: Int,
                        Name: String,
                        Sex: String,
                        Age: Double,
                        SibSp: Int,
                        Parch: Int,
                        Ticket: String,
                        Fare: Double,
                        Cabin: String,
                        Embarked: String
                      )

  val spark = SparkSession.builder()
    .appName("Titanic Dataset")
    .master("local[*]")
    .getOrCreate()

  val path = "C:\\Users\\C10123B\\Spark Practice\\Titanic"

  val schema = Encoders.product[Passenger].schema

  val titanicDF = spark.read
    .option("header", "true")
    .schema(schema)
    .csv(path)


  titanicDF.printSchema()
  titanicDF.show()

  val titanicDF2 = titanicDF.withColumn("fun", lit(null).cast("string"))
  titanicDF2.printSchema()
  titanicDF2.show()


  // Define connection properties
  val url = "jdbc:mysql://localhost:3306/FirstDB"
  val table = "FirstTable"
  val properties = new java.util.Properties()
  properties.put("user", "root")
  properties.put("password", "Abhi@1108")

  // Save the DataFrame to MySQL
  titanicDF2.write.mode("append")
    .jdbc(url, table, properties)

  // Stop the SparkSession
  spark.stop()


}
