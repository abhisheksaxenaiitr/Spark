import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}

object Work extends App {

  case class Data(canSignals: Seq[CanSignal])

  case class CanSignal(id: Int, issignal: Boolean, line: Option[Int], value: Value)

  case class Value(cnt: Option[Int],
                   codePage: Option[String],
                   dataBytesString: Option[String],
                   encodeCode: Option[String],
                   last: Option[Double],
                   sum: Option[Int],
                   text: Option[String],
                   `type`: Option[String])




  // Create a Spark session
  val spark = SparkSession.builder()
    .appName("Spark Case Class Example")
    .master("local[*]")
    .getOrCreate()

  // Defined the schema using the case classes
  val schema = Encoders.product[Data].schema

  // Read JSON data from a file
  val jsonPath = "C:\\Users\\C10123B\\JSON\\json"
  val df = spark.read.schema(schema).option("multiline",true)
    .json(jsonPath)
  df.printSchema()
  df.show()



  import spark.implicits._

  val final_df = df.withColumn("cansignal", explode($"canSignals"))
                                    .select(
                                      $"cansignal.id",
                                      $"cansignal.issignal",
                                      $"cansignal.line",
                                      $"cansignal.value.cnt",
                                      $"cansignal.value.codePage",
                                      $"cansignal.value.dataBytesString",
                                      $"cansignal.value.encodeCode",
                                      $"cansignal.value.last",
                                      $"cansignal.value.sum",
                                      $"cansignal.value.text",
                                      $"cansignal.value.type"
                                    )

  final_df.show(Int.MaxValue)
  println(s"Total Number of Rows are ${final_df.count()}")



  // Define connection properties
  val url = "jdbc:mysql://localhost:3306/Spark_Activity"
  val table = "Activity_1"
  val properties = new java.util.Properties()
  properties.put("user", "root")
  properties.put("password", "Abhi@1108")

  // Save the DataFrame to MySQL
  final_df.write.mode("append")
    .jdbc(url, table, properties)


 //Fetching Table directly from MySQL Database
  properties.setProperty("user", "root")
  properties.setProperty("password", "Abhi@1108")

  val fetch_df = spark.read.jdbc(url, table, properties)
  fetch_df.show()


}