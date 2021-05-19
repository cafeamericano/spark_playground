//import com.mongodb.client.{MongoClient, MongoClients}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.{SparkException, rdd}
import org.apache.spark.sql.{DataFrameWriter, SparkSession}
import org.apache.spark.sql.functions.{col, countDistinct, sumDistinct, current_date}
import org.bson.{BsonDocument, Document}
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions._

object Alternate extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("CitiesMongoSpark")
    .config("spark.mongodb.input.uri", "mongodb://192.168.86.40/test.cities")
    .config("spark.mongodb.output.uri", "mongodb://192.168.86.40/test.cities_output")
    .getOrCreate()

  import spark.implicits._

  val readConfigCities = ReadConfig(Map("collection" -> "cities", "readPreference.type" -> "secondaryPreferred"), Some(ReadConfig(spark.sparkContext)))
  val citiesRdd = MongoSpark.load(spark.sparkContext, readConfigCities)
  val citiesDf = citiesRdd.toDF()
    .filter("timestamp > DATE(NOW() - INTERVAL 7 DAY)")
    .groupBy($"name", $"population")
    .count()
    .agg(
      first("name"),
      sum("count"),
      collect_list("population") as "populations"
    )

  citiesDf.show()
  MongoSpark.save(citiesDf)

  spark.stop()

}