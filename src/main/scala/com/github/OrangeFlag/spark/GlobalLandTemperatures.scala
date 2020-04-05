package com.github.OrangeFlag.spark

import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object GlobalLandTemperatures {
  def calcAndSave(sparkSession: SparkSession, path: String, sqlConnectionUrl: String, sqlConnectionProperties: Properties): Unit = {
    import sparkSession.implicits._

    val Temperatures = sparkSession.read.format("csv").option("header", "true").load(path)
      .filter($"AverageTemperature".isNotNull)
      .select(
        $"city",
        $"country",
        year(to_date($"dt", "yyyy-MM-dd")) as "year",
        $"AverageTemperature".cast("double") as "temperature"
      )
      .withColumn("century", (($"year" - 1) / 100 + 1) cast "integer")
      .cache

    println(Temperatures.schema)

    val cityCenturyYearTemperatures = Temperatures.rollup("city", "century", "year").agg(
      min($"temperature") as "min_temperature",
      avg($"temperature") as "avg_temperature",
      max($"temperature") as "max_temperature",
      first($"country") as "country"
    ).cache


    val cityYearTemperatures = cityCenturyYearTemperatures.
      filter($"year".isNotNull).
      drop($"century")

    cityYearTemperatures.show()


    val cityCenturyTemperatures = cityCenturyYearTemperatures.
      filter($"century".isNotNull && $"year".isNull).
      drop($"year")

    cityCenturyTemperatures.show()

    val cityOverallTemperatures = cityCenturyYearTemperatures.
      filter($"city".isNotNull && $"century".isNull).
      drop($"century").
      drop($"year")

    cityOverallTemperatures.show()

    val countryYearTemperatures = cityYearTemperatures.groupBy("country", "year").agg(
      min($"min_temperature") as "min_temperature",
      max($"max_temperature") as "max_temperature"
    ).cache

    countryYearTemperatures.show()

    val countryCenturyTemperatures = countryYearTemperatures.
      withColumn("century", (($"year" - 1) / 100 + 1) cast "integer").
      groupBy("country", "century").
      agg(
        min($"min_temperature") as "min_temperature",
        max($"max_temperature") as "max_temperature"
      ).cache

    countryCenturyTemperatures.show()

    val countryOverallTemperatures = countryCenturyTemperatures.groupBy("country").agg(
      min($"min_temperature") as "min_temperature",
      max($"max_temperature") as "max_temperature"
    ).cache

    countryOverallTemperatures.show()

    cityYearTemperatures.write.mode(SaveMode.Overwrite).jdbc(sqlConnectionUrl, "cityYearTemperatures", sqlConnectionProperties)
    cityCenturyTemperatures.write.mode(SaveMode.Overwrite).jdbc(sqlConnectionUrl, "cityCenturyTemperatures", sqlConnectionProperties)
    cityOverallTemperatures.write.mode(SaveMode.Overwrite).jdbc(sqlConnectionUrl, "cityOverallTemperatures", sqlConnectionProperties)
    countryYearTemperatures.write.mode(SaveMode.Overwrite).jdbc(sqlConnectionUrl, "countryYearTemperatures", sqlConnectionProperties)
    countryCenturyTemperatures.write.mode(SaveMode.Overwrite).jdbc(sqlConnectionUrl, "countryCenturyTemperatures", sqlConnectionProperties)
    countryOverallTemperatures.write.mode(SaveMode.Overwrite).jdbc(sqlConnectionUrl, "countryOverallTemperatures", sqlConnectionProperties)
  }
}