import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object JSONObject extends App {
  val spark = SparkSession.builder()
    .appName("JSONObject")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val ordersDf = spark.read.format("json")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .load("C:\\Users\\C10123B\\JSON")
  ordersDf.show()
  ordersDf.printSchema()

  val parseOrdersDf = ordersDf.withColumn("orders", explode($"datasets"))
    .select($"orders.customerId", $"orders.orderId", $"orders.orderDate", $"orders.orderDetails", $"orders.shipmentDetails")
  parseOrdersDf.show()
  parseOrdersDf.printSchema()

  val explodedDf = parseOrdersDf.withColumn("orderDetails", explode($"orderDetails"))
    .select(
      $"customerId",
      $"orderId",
      $"orderDate",
      $"orderDetails.productId",
      $"orderDetails.quantity",
      $"orderDetails.sequence",
      $"orderDetails.totalPrice.gross",
      $"orderDetails.totalPrice.net",
      $"orderDetails.totalPrice.tax",
      $"shipmentDetails.city",
      $"shipmentDetails.country",
      $"shipmentDetails.postalCode",
      $"shipmentDetails.state",
      $"shipmentDetails.street"
    )

  println("This is for explodedDf")
  explodedDf.show()
  explodedDf.printSchema()

  val jsonParseOrdersDf = explodedDf.select(
    $"orderId",
    $"customerId",
    $"orderDate",
    $"productId",
    $"quantity",
    $"sequence",
    $"gross",
    $"net",
    $"tax",
    $"street",
    $"city",
    $"state",
    $"postalCode",
    $"country"
  )

  jsonParseOrdersDf.show()
}
