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

  val readConfigCities = ReadConfig(Map("collection" -> "cities", "readPreference.type" -> "secondaryPreferred"), Some(ReadConfig(spark.sparkContext)))
  val citiesRdd = MongoSpark.load(spark.sparkContext, readConfigCities)
  val citiesDf = citiesRdd.toDF()
  citiesDf.show()
  citiesDf.createOrReplaceTempView("cities")

  val readConfigStates = ReadConfig(Map("collection" -> "states", "readPreference.type" -> "secondaryPreferred"), Some(ReadConfig(spark.sparkContext)))
  val statesRdd = MongoSpark.load(spark.sparkContext, readConfigStates)
  val statesDf = statesRdd.toDF()
  statesDf.show()
  statesDf.createOrReplaceTempView("states")

  val results = spark.sql(
    """
      |SELECT
        |states.name,
        |count(*),
        |sum(population)
      |FROM cities
      |LEFT JOIN states ON cities.state = states.abbr
      |WHERE population > 100000
      |GROUP BY states.name
    |""".stripMargin
  )

  results.show()
  MongoSpark.save(results)
  spark.stop()

}