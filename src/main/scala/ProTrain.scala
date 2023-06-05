import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}

object ProTrain extends App {

  case class FullData(data: Data,
                      realtime: Option[String],
                      seed: String)

  case class Data(
                   compatibility: Option[String],
                   deviceid: String,
                   events: Events,
                   mileage: Option[String],
                   networkinfo: NetworkInfo,
                   pos: Pos,
                   reports: Reports,
                   status: Status,
                   time: String
                 )



  case class Events(displayAlarms: DisplayAlarms, harvesterFaults: Seq[HarvestingFaults])
  case class DisplayAlarms(alarms: Seq[Alarm], version: Int)
  case class Alarm(severity: String, active: Boolean, fault: String, `type`: String)
  case class HarvestingFaults(ecu: Int, faults: Seq[Faults])
  case class Faults(spn: Int, fmi: Int, priority: Int, bypass: Int)

  case class NetworkInfo(MCC: String,
                         MNC: String,
                         NetworkStatus: String,
                         connection: String,
                         operatorName: String,
                         rssi: Int)

  case class Pos(alt: Int,
                 current: Boolean,
                 direction: Float,
                 fixtype: Int,
                 lat: Double,
                 lon: Double,
                 pdop: Int,
                 satcount: Int,
                 speed: Float,
                 time: String)

  case class Reports(canReports: CanReports, dutyReports: Seq[DutyReports])
  case class CanReports(canSignals: Seq[CanSignal], duration: Int, version: Option[String])
  case class CanSignal(id: Int, issignal: Boolean, line: Option[Int], value: Value)

  case class Value(cnt: Option[Int],
                   codePage: Option[String],
                   dataBytesString: Option[String],
                   encodeCode: Option[String],
                   last: Option[Double],
                   sum: Option[Int],
                   text: Option[String],
                   `type`: Option[String])

  case class DutyReports(duration: Int,
                         duties: Seq[Duties],
                         reportid: Int,
                         version: Option[String])

  case class Duties(duration: Int, duty: Int)
  case class Status(device: String, duty: Int)




  // Create a Spark session
  val spark = SparkSession.builder()
    .appName("Spark Case Class Example")
    .master("local[*]")
    .getOrCreate()

  // Define the schema using the case classes
  val schema = Encoders.product[FullData].schema

  // Read JSON data from a file
  val jsonPath = "C:\\Users\\C10123B\\JSON\\message"
  val df = spark.read.schema(schema)
    .option("multiline", true)
    .json(jsonPath)

  df.printSchema()
  df.show()


  import spark.implicits._
  val exploded_df = df.withColumn("Data",$"data").select(
    $"Data.compatibility",
    $"Data.deviceid",
    $"Data.events.displayAlarms.alarms".alias("alarm"),              //Explode
    $"Data.events.displayAlarms.version".alias("displayAlarms_version"),
    $"Data.events.harvesterFaults".alias("hfault"),                   //Explode
    $"Data.mileage",
    $"Data.networkinfo.MCC",
    $"Data.networkinfo.MNC",
    $"Data.networkinfo.NetworkStatus",
    $"Data.networkinfo.connection",
    $"Data.networkinfo.operatorName",
    $"Data.networkinfo.rssi",
    $"Data.pos.alt",
    $"Data.pos.current",
    $"Data.pos.direction",
    $"Data.pos.fixtype",
    $"Data.pos.lat",
    $"Data.pos.lon",
    $"Data.pos.pdop",
    $"Data.pos.satcount",
    $"Data.pos.speed",
    $"Data.pos.time".alias("pos_time"),
    $"Data.reports.canReports.canSignals".alias("csignal"),                //Explode
    $"Data.reports.canReports.duration",
    $"Data.reports.canReports.version",
    $"Data.reports.dutyReports".alias("dreports"),                          //Explode
    $"Data.status.device",
    $"Data.status.duty",
    $"Data.time"
  )

  exploded_df.show()

  val parsed_df1 = exploded_df.withColumn("alarms", explode($"alarm")).select(
    $"compatibility",
    $"deviceid",
    $"alarms.severity",
    $"alarms.active",
    $"alarms.fault",
    $"alarms.type",
    $"displayAlarms_version",
    $"hfault", //Explode
    $"mileage",
    $"MCC",
    $"MNC",
    $"NetworkStatus",
    $"connection",
    $"operatorName",
    $"rssi",
    $"alt",
    $"current",
    $"direction",
    $"fixtype",
    $"lat",
    $"lon",
    $"pdop",
    $"satcount",
    $"speed",
    $"pos_time",
    $"csignal", //Explode
    $"duration",
    $"version",
    $"dreports", //Explode
    $"device",
    $"duty",
    $"time"
  )

  parsed_df1.show()

  val parsed_df2 = parsed_df1.withColumn("harvesterFaults", explode($"hfault")).select(
    $"compatibility",
    $"deviceid",
    $"severity",
    $"active",
    $"fault",
    $"type",
    $"displayAlarms_version",
    $"harvesterFaults.ecu",
    $"harvesterFaults.faults".alias("harvester_fault"),                                       //explode
    $"mileage",
    $"MCC",
    $"MNC",
    $"NetworkStatus",
    $"connection",
    $"operatorName",
    $"rssi",
    $"alt",
    $"current",
    $"direction",
    $"fixtype",
    $"lat",
    $"lon",
    $"pdop",
    $"satcount",
    $"speed",
    $"pos_time",
    $"csignal", //Explode
    $"duration",
    $"version",
    $"dreports", //Explode
    $"device",
    $"duty",
    $"time"
  )

  parsed_df2.show()

  val parsed_df3 = parsed_df2.withColumn("harvester_fault", explode($"harvester_fault")).select(
    $"compatibility",
    $"deviceid",
    $"severity",
    $"active",
    $"fault",
    $"type",
    $"displayAlarms_version",
    $"ecu",
    $"harvester_fault.spn",
    $"harvester_fault.fmi",
    $"harvester_fault.priority",
    $"harvester_fault.bypass",
    $"mileage",
    $"MCC",
    $"MNC",
    $"NetworkStatus",
    $"connection",
    $"operatorName",
    $"rssi",
    $"alt",
    $"current",
    $"direction",
    $"fixtype",
    $"lat",
    $"lon",
    $"pdop",
    $"satcount",
    $"speed",
    $"pos_time",
    $"csignal", //Explode
    $"duration",
    $"version",
    $"dreports", //Explode
    $"device",
    $"duty",
    $"time"
  )

  parsed_df3.show

  val parsed_df4 = parsed_df3.withColumn("csignal", $"csignal").select(
    $"compatibility",
    $"deviceid",
    $"severity",
    $"active",
    $"fault",
    $"type".alias("alarms_type"),
    $"displayAlarms_version",
    $"ecu",
    $"spn",
    $"fmi",
    $"priority",
    $"bypass",
    $"mileage",
    $"MCC",
    $"MNC",
    $"NetworkStatus",
    $"connection",
    $"operatorName",
    $"rssi",
    $"alt",
    $"current",
    $"direction",
    $"fixtype",
    $"lat",
    $"lon",
    $"pdop",
    $"satcount",
    $"speed",
    $"pos_time",
    $"csignal.id",
    $"csignal.issignal",
    $"csignal.line",
    $"csignal.value.cnt",
    $"csignal.value.codePage",
    $"csignal.value.dataBytesString",
    $"csignal.value.encodeCode",
    $"csignal.value.last",
    $"csignal.value.sum",
    $"csignal.value.text",
    $"csignal.value.type",
    $"duration",
    $"version",
    $"dreports", //Explode
    $"device",
    $"duty",
    $"time"
  )

  parsed_df4.show()

  val parsed_df5 = parsed_df4.withColumn("dutyReports", explode($"dreports")).select(
    $"compatibility",
    $"deviceid",
    $"severity",
    $"active",
    $"fault",
    $"alarms_type",
    $"displayAlarms_version",
    $"ecu",
    $"spn",
    $"fmi",
    $"priority",
    $"bypass",
    $"mileage",
    $"MCC",
    $"MNC",
    $"NetworkStatus",
    $"connection",
    $"operatorName",
    $"rssi",
    $"alt",
    $"current",
    $"direction",
    $"fixtype",
    $"lat",
    $"lon",
    $"pdop",
    $"satcount",
    $"speed",
    $"pos_time",
    $"id",
    $"issignal",
    $"line",
    $"cnt",
    $"codePage",
    $"dataBytesString",
    $"encodeCode",
    $"last",
    $"sum",
    $"text",
    $"type",
    $"duration".alias("dutyReports_duration"),
    $"version".alias("canReports_version"),
    $"dutyReports.duration",
    $"dutyReports.duties",                                                //explode
    $"dutyReports.reportid",
    $"dutyReports.version".alias("dutyReports_version"),
    $"device",
    $"duty",
    $"time"
  )

  parsed_df5.show()


  val parsed_df6 = parsed_df5.withColumn("duties", explode($"duties")).select(
    $"compatibility",
    $"deviceid",
    $"severity",
    $"active",
    $"fault",
    $"alarms_type",
    $"displayAlarms_version",
    $"ecu",
    $"spn",
    $"fmi",
    $"priority",
    $"bypass",
    $"mileage",
    $"MCC",
    $"MNC",
    $"NetworkStatus",
    $"connection",
    $"operatorName",
    $"rssi",
    $"alt",
    $"current",
    $"direction",
    $"fixtype",
    $"lat",
    $"lon",
    $"pdop",
    $"satcount",
    $"speed",
    $"pos_time",
    $"id",
    $"issignal",
    $"line",
    $"cnt",
    $"codePage",
    $"dataBytesString",
    $"encodeCode",
    $"last",
    $"sum",
    $"text",
    $"type",
    $"duration",
    $"canReports_version",
    $"duration",
    $"duties.duration".alias("duties_duration"),
    $"duties.duty",
    $"reportid",
    $"dutyReports_version",
    $"device",
    $"duty".alias("status_duty"),
    $"time"
  )

  parsed_df6.show()

  val parsed_df7 = parsed_df6.select(
    $"compatibility",
    $"deviceid",
    $"severity",
    $"active",
    $"fault",
    $"alarms_type",
    $"displayAlarms_version",
    $"ecu",
    $"spn",
    $"fmi",
    $"priority",
    $"bypass",
    $"mileage",
    $"MCC",
    $"MNC",
    $"NetworkStatus",
    $"connection",
    $"operatorName",
    $"rssi",
    $"alt",
    $"current",
    $"direction",
    $"fixtype",
    $"lat",
    $"lon",
    $"pdop",
    $"satcount",
    $"speed",
    $"pos_time",
    $"id",
    $"issignal",
    $"line",
    $"cnt",
    $"codePage",
    $"dataBytesString",
    $"encodeCode",
    $"last",
    $"sum",
    $"text",
    $"type",
    $"duration",
    $"canReports_version",
    $"duration",
    $"duties_duration",
    $"duty",
    $"reportid",
    $"dutyReports_version",
    $"device",
    $"status_duty",
    $"time"
  )

  parsed_df7.show()



}