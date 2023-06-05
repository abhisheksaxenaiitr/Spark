import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object Test extends App{

  val spark = SparkSession.builder()
    .appName("JSONObject")
    .master("local[*]")
    .getOrCreate()


  // Define the schema for the nested "events" field
  val alarmsSchema = StructType(Seq(
    StructField("severity", StringType, nullable = true),
    StructField("active", BooleanType, nullable = true),
    StructField("fault", StringType, nullable = true),
    StructField("type", StringType, nullable = true)
  ))

  // Define the schema for the "harvesterFaults" field
  val faultsSchema = StructType(Seq(
    StructField("ecu", IntegerType, nullable = true),
    StructField("faults", ArrayType(StructType(Seq(
      StructField("spn", IntegerType, nullable = true),
      StructField("fmi", IntegerType, nullable = true),
      StructField("priority", IntegerType, nullable = true),
      StructField("bypass", IntegerType, nullable = true)
    ))), nullable = true)
  ))

  // Define the schema for the "networkinfo" field
  val networkInfoSchema = StructType(Seq(
    StructField("MCC", StringType, nullable = true),
    StructField("MNC", StringType, nullable = true),
    StructField("NetworkStatus", StringType, nullable = true),
    StructField("connection", StringType, nullable = true),
    StructField("operatorName", StringType, nullable = true),
    StructField("rssi", IntegerType, nullable = true)
  ))

  // Define the schema for the "pos" field
  val posSchema = StructType(Seq(
    StructField("alt", IntegerType, nullable = true),
    StructField("current", BooleanType, nullable = true),
    StructField("direction", DoubleType, nullable = true),
    StructField("fixtype", IntegerType, nullable = true),
    StructField("lat", DoubleType, nullable = true),
    StructField("lon", DoubleType, nullable = true),
    StructField("pdop", IntegerType, nullable = true),
    StructField("satcount", IntegerType, nullable = true),
    StructField("speed", DoubleType, nullable = true),
    StructField("time", StringType, nullable = true)
  ))

  // Define the schema for the "value" field within "canSignals"
  val canValueSchema = StructType(Seq(
    StructField("cnt", IntegerType, nullable = true),
    StructField("codePage", IntegerType, nullable = true),
    StructField("dataBytesString", StringType, nullable = true),
    StructField("encodeCode", IntegerType, nullable = true),
    StructField("last", IntegerType, nullable = true),
    StructField("sum", IntegerType, nullable = true),
    StructField("text", StringType, nullable = true),
    StructField("type", StringType, nullable = true)
  ))

  // Define the schema for "canSignals"
  val canSignalsSchema = StructType(Seq(
    StructField("id", IntegerType, nullable = true),
    StructField("issignal", BooleanType, nullable = true),
    StructField("line", IntegerType, nullable = true),
    StructField("value", canValueSchema, nullable = true)
  ))

  // Define the main schema for the entire JSON structure
  val schema = StructType(Seq(
    StructField("data", StructType(Seq(
      StructField("compatibility", StringType, nullable = true),
      StructField("deviceid", StringType, nullable = true),
      StructField("events", StructType(Seq(
        StructField("alarms", ArrayType(alarmsSchema), nullable = true),
        StructField("faults", faultsSchema, nullable = true)
      )), nullable = true),
      StructField("harvesterFaults", ArrayType(IntegerType), nullable = true),
      StructField("networkinfo", networkInfoSchema, nullable = true),
      StructField("pos", posSchema, nullable = true),
      StructField("rec", StringType, nullable = true),
      StructField("timestamp", StringType, nullable = true),
      StructField("uid", StringType, nullable = true),
      StructField("userid", StringType, nullable = true),
      StructField("canSignals", ArrayType(canSignalsSchema), nullable = true)
    )), nullable = true),
    StructField("meta", StructType(Seq(
      StructField("version", StringType, nullable = true)
    )), nullable = true)
  )
  )

  // Read the JSON data with the defined schema
  val jsonData = spark.read.schema(schema).json("C:\\Users\\C10123B\\JSON\\message")
  jsonData.printSchema()
  jsonData.show()

}

// Perform operations on the DataFrame as needed


/*// Create the DataFrame using the schema
val ordersDf = spark.read.format("json")
  .schema(schema)
  .option("multiLine", "true")
  .load("C:\\Users\\C10123B\\JSON\\message")
ordersDf.show()
ordersDf.printSchema()*/

