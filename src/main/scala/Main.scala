//import com.mongodb.client.{MongoClient, MongoClients}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.{SparkException, rdd}
import org.apache.spark.sql.{DataFrameWriter, SparkSession}
import org.apache.spark.sql.functions.{col, countDistinct, sumDistinct, current_date}

object Main extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("CitiesMongoSpark")
    .config("spark.mongodb.input.uri", "mongodb://localhost/test.cities")
    .config("spark.mongodb.output.uri", "mongodb://localhost/test.cities_output")
    .getOrCreate()

  import spark.implicits._

  val readConfig = ReadConfig(Map("collection" -> "cities", "readPreference.type" -> "secondaryPreferred"), Some(ReadConfig(spark.sparkContext)))
  val citiesRdd = MongoSpark.load(spark.sparkContext, readConfig)
  val citiesDf = citiesRdd.toDF()
  citiesDf.createOrReplaceTempView("cities")
  citiesDf.select(countDistinct("name")).show(true)
  val results = spark.sql(
    """
      |SELECT
        |state,
        |count(*),
        |sum(population)
      |FROM cities
      |WHERE population > 100000
      |GROUP BY state
    |""".stripMargin
  )
  results.show()
  MongoSpark.save(results)
  spark.stop()
}