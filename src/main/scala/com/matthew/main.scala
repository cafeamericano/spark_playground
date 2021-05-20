package com.matthew

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

object main extends App {

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

  // Define the count of all results from the query
  val countAllResults = results.count()

  // Filter out results where the first column isn't Georgia
  val filterResults = results.filter(x => {
    print(x)
    print(x(0))
    x(0) == "Georgia"
  })

  // Define the count of filtered results, then show
  val countFilteredResults = filterResults.count()
  filterResults.show()

  // Add a timestamp column to the table
  val expandedDf = filterResults.withColumn("timestamp", current_timestamp())
  val updatedDf = expandedDf.withColumn("name", regexp_replace(col("name"), "a", "A"))

  // Save all of the tables to the DB
  MongoSpark.save(results)
  MongoSpark.save(filterResults)
  MongoSpark.save(expandedDf)
  MongoSpark.save(updatedDf)

  spark.stop()

}
