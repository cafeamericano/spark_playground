package com.matthew

//import com.mongodb.client.{MongoClient, MongoClients}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object traffic extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("TrafficMongoSpark")
    .config("spark.mongodb.input.uri", "mongodb://192.168.86.40/test.traffic")
    .config("spark.mongodb.output.uri", "mongodb://192.168.86.40/test.traffic_report")
    .getOrCreate()

  import spark.implicits._

  val readConfigTraffic = ReadConfig(Map("collection" -> "traffic", "readPreference.type" -> "secondaryPreferred"), Some(ReadConfig(spark.sparkContext)))
  val trafficRdd = MongoSpark.load(spark.sparkContext, readConfigTraffic)
  val trafficDf = trafficRdd.toDF()
    .filter("timestamp > DATE(NOW() - INTERVAL 7 DAY)")
    .groupBy($"ip_address", $"browser", $"sublocation", $"description", $"operation")
    .count()
    .agg(
      sum("count") as "all_interactions_count",
      collect_set("ip_address") as "unique_users",
      collect_set("browser") as "unique_browsers",
      collect_set("sublocation") as "unique_sublocations",
      collect_set("description") as "unique_descriptions",
      collect_set("operation") as "unique_operations",
      collect_list("ip_address") as "users",
      collect_list("browser") as "browsers"
    )
    .withColumn("unique_visitor_count", size($"unique_users"))
    .withColumn("unique_visitor_count", size($"unique_users"))
    .withColumn("run_date", current_timestamp())
    .withColumn("report_start_date", date_add(current_timestamp(), - 7))
    .withColumn("report_end_date", current_timestamp())

  trafficDf.show()
  MongoSpark.save(trafficDf)

  spark.stop()

}
