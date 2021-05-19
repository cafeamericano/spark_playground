package com.matthew

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp, regexp_replace}

object main extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("CitiesMongoSpark")
    .config("spark.mongodb.input.uri", "mongodb://192.168.86.40/test.cities")
    .config("spark.mongodb.output.uri", "mongodb://192.168.86.40/test.cities_output")
    .getOrCreate()

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
