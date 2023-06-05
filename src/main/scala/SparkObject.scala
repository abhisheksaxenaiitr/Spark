import org.apache.spark.sql.functions.{col, concat, expr, lit, when}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}

import scala.language.postfixOps

object SparkObject extends App {


    val spark = SparkSession.builder()
      .appName("Read CSV file")
      .master("local[*]")
      .getOrCreate()



    val path = "C:\\Users\\C10123B\\Spark Practice"

    val df = spark.read
      .option("header", "true")
      .csv(path)

    //df.show(Int.MaxValue)
    df.printSchema()

    //Renaming A Column
    val df2 = df.withColumnRenamed("EMPLOYEE_ID", "EID")
    df2.printSchema()

    val df3 = df2.withColumnRenamed("FIRST_NAME", "F_NAME")
      .withColumnRenamed("LAST_NAME", "L_NAME")
    df3.printSchema()

    val data = Seq(Row(Row("James","","Smith"),"36636","M","3000"),
        Row(Row("Michael","Rose",""),"40288","M","4000"),
        Row(Row("Robert","","Williams"),"42114","M","4000"),
        Row(Row("Maria","Anne","Jones"),"39192","F","4000"),
        Row(Row("Jen","Mary","Brown"),"","F","-1")
    )

    //Updating Column In Same Dataframe
    val df4 = df.withColumn("SALARY",col("SALARY")* 1.5)
    df4.show()

    //Getting A New Dataframe With Some Updation In Base Dataframe's Column
    val df5 = df.withColumn("NEW SALARY",col("SALARY")* 1.5)
    df5.show()

    //Updating Multiple Columns
    val df6 = df.selectExpr(
        "EMPLOYEE_ID",
            "FIRST_NAME",
            "LAST_NAME",
            "EMAIL",
            "SALARY * 1.5 as SALARY",
            "DEPARTMENT_ID"
    )
    df6.show()

    df.createOrReplaceTempView("EMPLOYEE")
    val dff = spark.sql("SELECT SALARY*1.5 AS SALARY, FIRST_NAME FROM EMPLOYEE")
    dff.show()


    // Adding Kumar at the end of values of the LAST_NAME in df Dataframe
    val df7 = df.withColumn("LAST_NAME", functions.concat(df("LAST_NAME"), lit(" Kumar")))
    df7.show()

    //Adding Kumar at the beginning of values of the LAST_NAME in df Dataframe
    val df8 = df.withColumn("LAST_NAME", functions.concat(lit("KUMAR "), df("LAST_NAME")))
    df8.show()

    //Dropping A Column
    val df9 = df.drop("COMMISSION_PCT")
    df9.show()


    // Merging FIRST_NAME and LAST_NAME columns to NAME in df DataFrame
    val dfMerged = df.withColumn("NAME", concat(col("FIRST_NAME"), lit(" "), col("LAST_NAME")))
      .drop("FIRST_NAME","LAST_NAME")

    dfMerged.show()


    //Splitting NAME column to FIRST_NAME and LAST_NAME in df Dataframe
    val df10 = dfMerged.select(functions.split(col("NAME"), " ").getItem(0).as("FIRST_NAME"),
        functions.split(col("NAME"), " ").getItem(1).as("LAST_NAME"))
      .drop("NAME")

    df10.show(false)


    //Introducing a new column RATED SALARY in the Dataframe df
    val df11 = dfMerged.withColumn("RATED SALARY", when(col("SALARY") >= 12500, "Good Salary")
      .otherwise("Insufficient Salary"))

    df11.show()

    //Updating the column SALARY with condition and getting new Dataframe
    val df12 = dfMerged.withColumn("SALARY", when(col("SALARY") >= 12500, "Good Salary")
      .otherwise("Insufficient Salary"))

    df12.show()






}


