package scala.spark.utilities

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

/*
 * Trait to start and stop Spark session
 *
 * @author - Sandeep
 *
 */
 
trait SparkConfiguration {

 //Define a spark session
  lazy val spark =
    SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()


   //Method to stop the spark session
  def stopSpark(): Unit = spark.stop()
}