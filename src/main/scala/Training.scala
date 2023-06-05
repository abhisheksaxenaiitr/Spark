import org.apache.spark.sql.{SparkSession, Encoders}
import org.apache.spark.sql.types._

object Training extends App {
  // Define case classes for the nested structure
  case class DeviceData(
                         compatibility: Option[String],
                         deviceid: String,
                         events: Events,
                         harvesterFaults: HarvesterFault,
                         mileage: Option[Int],
                         networkinfo: NetworkInfo,
                         pos: Position,
                         reports: Reports
                       )

  case class Events(displayAlarms: DisplayAlarms, version: Int)
  case class DisplayAlarms(alarms: Seq[Alarm])
  case class Alarm(severity: String, active: Boolean, fault: String, `type`: String)
  case class HarvesterFault(ecu: Int, faults: Seq[Fault])
  case class Fault(spn: Int, fmi: Int, priority: Int, bypass: Int)
  case class NetworkInfo(MCC: String, MNC: String, NetworkStatus: String, connection: String, operatorName: String, rssi: Int)
  case class Position(alt: Int, current: Boolean, direction: Double, fixtype: Int, lat: Double, lon: Double, pdop: Int, satcount: Int, speed: Double, time: String)
  case class Reports(canReports: CANReports)
  case class CANReports(canSignals: Seq[CANSignal])
  case class CANSignal(id: Int, issignal: Boolean, line: Option[Int], value: CANSignalValue)
  case class CANSignalValue(cnt: Option[Int], codePage: Option[String], dataBytesString: Option[String], encodeCode: Option[String], last: Option[Double], sum: Option[Int], text: Option[String], `type`: Option[String])


  // Create a Spark session
  val spark = SparkSession.builder()
    .appName("Spark Case Class Example")
    .master("local[*]")
    .getOrCreate()

  // Define the schema using the case classes
  val schema = Encoders.product[DeviceData].schema

  // Read JSON data from a file
  val jsonPath = "C:\\Users\\C10123B\\JSON\\message"
  val df = spark.read.schema(schema).json(jsonPath)
  df.printSchema()
}