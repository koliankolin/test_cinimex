import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ColumnsAndExpressions extends App {

  val spark = SparkSession
    .builder()
    .appName("Columns")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("OFF")

  val logsSchema = StructType(
    Array(
      StructField("type", StringType),
      StructField("date", DateType),
      StructField("value", StringType),
    )
  )

  // 1
  val logs_ = spark.read
    .schema(logsSchema)
    .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
    .option("header", "false")
    .option("compression", "gzip")
    .csv("src/data/ghtorrent-logs.txt.gz")
    .withColumn("_tmp", split(col("value"), " -- "))
    .select(
      col("type"),
      col("date"),
      trim(col("_tmp").getItem(0)).as("client"),
      col("_tmp").getItem(1).as("value"),
    )

  val logs = logs_
    .withColumn("_tmp", split(col("value"), ".rb: "))
    .select(
      col("type"),
      col("date"),
      col("client"),
      trim(col("_tmp").getItem(0)).as("value_type"),
      trim(col("_tmp").getItem(1)).as("value"),
    )

  // 2
  println(s"There are ${logs.count()} rows in file")

  // 3
  logs.filter("type = 'WARN'")
    .select(
      count("type").as("count_warnings")
    ).show()

  // 4
  println(logs.filter("value_type = 'api_client.rb'")
    .count()
  )

  // 5
  logs
    .filter("value_type = 'api_client'")
    .groupBy("client")
    .agg(
      count("*").as("count_http")
    )
    .orderBy(desc("count_http"))
    .limit(1)
    .show()

  // 6
  logs
    .filter("value_type = 'api_client'")
    .filter(lower(col("value")).like("%fail%"))
    .groupBy("client")
    .agg(
      count("*").as("count_http")
    )
    .orderBy(desc("count_http"))
    .limit(1)
    .show()

  // 7

}