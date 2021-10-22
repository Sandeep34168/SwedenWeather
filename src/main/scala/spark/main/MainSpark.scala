package scala.spark.main

import org.apache.log4j.Logger
import scala.spark.utilities.SparkConfigUtility
import scala.spark.pressure.PressureSpark
import scala.spark.temperature.TemperatureSpark



/**
 * Spark program that transforms temperature data, pressure data and stores it in hive table.
 * This class contains main method, entry point to the spark application.
 *
 * @author - Sandeep
 */

object MainSpark extends SparkConfiguration {

  // logger
  val log = Logger.getLogger(getClass.getName)


  //Entry point to the application
  def main(args: Array[String]) {

    // Naming spark program
    spark.conf.set("spark.app.name", "Pressure and Temperature analysis")

    //pressure data transformation
    log.info("Pressure data transformation and analysis started")
    pressureAnalysis(spark)

    //temperature data transformation
    log.info("Temperature data transformation and analysis started")
    temperatureAnalysis(spark)

    //stop the spark session
    stopSpark()
  }

  }