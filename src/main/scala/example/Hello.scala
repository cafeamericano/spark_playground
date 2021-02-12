package com.test

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SparkSession

object FirstMongoSparkApp extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("MongoSparkProject")
    .config("spark.mongodb.input.uri", "mongodb://localhost/test.cities")
    .config("spark.mongodb.output.uri", "mongodb://localhost/test.outputCities")
    .getOrCreate()

  import spark.implicits._

  val readConfig = ReadConfig(Map("collection" -> "cities", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(spark.sparkContext)))
  val customRdd = MongoSpark.load(spark.sparkContext, readConfig)

  MongoSpark.save(customRdd)

  customRdd.toDF().show()
  
  spark.stop()

}
