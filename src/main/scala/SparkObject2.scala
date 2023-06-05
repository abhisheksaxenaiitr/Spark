import org.apache.spark.sql.functions.{col, concat, expr, lit, when}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}

import scala.language.postfixOps

object SparkObject2 extends App {


  val spark = SparkSession.builder()
    .appName("Read CSV file")
    .master("local[*]")
    .getOrCreate()


  val path = "C:\\Users\\C10123B\\Datasets\\Titanic"

  val df = spark.read
    .option("header", "true")
    .csv(path)

  println(s"Total number of rows is ${df.count()}")
  df.show(Int.MaxValue)

  val df1 = df.filter(df("Age").isNull)
  df1.show(truncate = true,numRows = Int.MaxValue)
  println(s"Total number of rows with null value for Age are ${df1.count()}")


  //Dropping all Rows with NULL values
  val dfWithoutNull = df.na.drop()
  dfWithoutNull.show()

  //Dropping rows with NULL values corresponding to a specific column
  val dfWithoutNull2 = df.na.drop("any",Seq("Age"))
  dfWithoutNull2.show()
  println(s"No. of non null rows considering null values of Age column are ${dfWithoutNull2.count()}")

  //Dropping rows with Null values corresponding to mulltiple columns
  val dfWithoutNull3 = df.na.drop("any", Seq("Age", "Embarked"))
  dfWithoutNull3.show()
  println(s"No. of non null rows considering null values of Age and Embarked column are ${dfWithoutNull3.count()}")

}