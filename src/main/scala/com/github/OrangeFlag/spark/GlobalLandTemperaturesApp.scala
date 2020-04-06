package com.github.OrangeFlag.spark


import java.util.Properties
import org.apache.spark.sql.SparkSession


object GlobalLandTemperaturesApp extends App {

  val sparkSession = SparkSession.builder.master("local").appName(this.getClass.getSimpleName).getOrCreate()
  sparkSession.sparkContext.setLogLevel("WARN")

  private val SQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/dbname"
  private val SQL_CONNECTION_PROPERTIES = new Properties()
  SQL_CONNECTION_PROPERTIES.put("user", "username")
  SQL_CONNECTION_PROPERTIES.put("password", "yourpassword")

  GlobalLandTemperatures.calcAndSave(sparkSession, getClass.getResource("/GlobalLandTemperaturesByMajorCity.csv").toURI.toString, SQL_CONNECTION_URL, SQL_CONNECTION_PROPERTIES)
}
