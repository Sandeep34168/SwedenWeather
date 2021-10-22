package scala.spark.temperature

import java.io.FileNotFoundException
import scala.util.Failure
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import scala.spark.utilities.SparkConfiguration
import spark.sql
import scala.weather.schema.temperature.TemperatureSchema.ActualSchema
import scala.weather.schema.temperature.TemperatureSchema.UncleanSchema
import scala.weather.schema.temperature.TemperatureSchema.AutomaticSchema
import scala.weather.schema.temperature.TemperatureSchema.ManualSchema
import scala.weather.utils.temperature.TemperatureUtils

/**
 * Spark program that transforms temperature data and stores it in hive table.
 *
 * @author - Sandeep
 */

Class TemperatureSpark extends SparkConfiguration {

  // logger
  val log = Logger.getLogger(getClass.getName)


  /*
   * Temperature Data Cleaning
   *
   * @param sparkSession
   */
  def temperatureAnalysis(sparkSession: SparkSession): Unit = {

    // Reading property file
    val tempUtils = new temperatureUtils()
    val properties = tempUtils.readPropertyFile()

    try {

      //---------------------------Manual Station Temperature Data---------------------------------

      // Reading input data
      val manualStationTempRDD = sparkSession.sparkContext
        .textFile(properties.getProperty("temperature.manual.input.dir"))
        .map(item => item.split("\\s+"))
        .map(column =>
          ManualSchema(
            column(0), column(1), column(2), column(3), column(4), column(5), column(6), column(7), column(8)))
      val manualStationTempDF = sparkSession.createDataFrame(manualStationTempRDD)

      // Add station column
      val manualStationDF = manualStationTempDF.withColumn("station", lit("manual"))


      //-------------------------Automatic Station Temperature Data------------------------------------

      // Read automatic reading temperature data
      val automaticStationTempRDD = sparkSession.sparkContext
        .textFile(properties.getProperty("temperature.automatic.input.dir"))
        .map(item => item.split("\\s+"))
        .map(column => AutomaticSchema(
          column(0), column(1), column(2), column(3), column(4), column(5), column(6), column(7), column(8)))
      val automaticStationTempDF = sparkSession.createDataFrame(automaticStationTempRDD)

      // Add station column
      val automaticStationDF = manualStationTempDF.withColumn("station", lit("automatic"))


      //-----------------------------Space contained temperature data----------------------------------

      //Reading input data
      val spaceTempDataRDD = sparkSession.sparkContext
        .textFile(properties.getProperty("temperature.space.input.dir"))
        .map(item => item.split("\\s+"))
        .map(column => UncleanSchema(column(0), column(1), column(2), column(3), column(4), column(5), column(6)))
      val uncleanTempDataDF = sparkSession.createDataFrame(uncleanTempDataRDD)

      // Add or remove necessary columns in the dataframe
      val uncleanTempCleansedDF = uncleanTempDataDF
        .drop("extra")
        .withColumn("minimum", lit("NaN"))
        .withColumn("maximum", lit("NaN"))
        .withColumn("estimatedDiurnalMean", lit("NaN"))
        .withColumn("station", lit("NaN"))


      //---------------------------Read non changed schema temperature data-----------------------------

      //Reading input data
      val temperatureDataRDD = sparkSession.sparkContext
        .textFile(properties.getProperty("temperature.actual.input.dir"))
        .map(item => item.split("\\s+"))
        .map(column => ActualSchema(column(0), column(1), column(2), column(3), column(4), column(5)))

      //creating dataframe and adding necessary columns
      val temperatureDataTempDF = sparkSession.createDataFrame(temperatureDataRDD)
      val temperatureDataDF = temperatureDataTempDF
        .withColumn("minimum", lit("NaN"))
        .withColumn("maximum", lit("NaN"))
        .withColumn("estimatedDiurnalMean", lit("NaN"))
        .withColumn("station", lit("NaN"))

      // Joining all the input data to make as one final data frame
      val temperatureDF = manualStationDF
        .union(automaticStationDF)
        .union(uncleanTempCleansedDF)
        .union(temperatureDataDF)


      //----------------------------Save temperature data to hive table-----------------------------


      // Creating hive table
      sql("""CREATE TABLE TemperatureData(
        year String, 
        month String, 
        day String, 
        morning String, 
        noon String, 
        evening String, 
        minimum String,
        maximum String,
        estimated_diurnal_mean String, 
        station String)
      STORED AS PARQUET""")

      // Writing to hive table created above.
      temperatureDF.write.mode(SaveMode.Overwrite).saveAsTable("TemperatureData")


      //---------------------------------------Data Analysis---------------------------------

      val totalInputCount = manualStationTempRDD.count() +
        automaticStationTempRDD.count() +
        spaceTempDataRDD.count() +
        temperatureDataRDD.count()

      log.info("Input data count is " + totalInputCount)
      log.info("Transformed input data count is " + temperatureDF.count())
      log.info("Hive data count " + sql("SELECT count(*) FROM TemperatureData").show(false))

    } catch {
      case fileNotFoundException: FileNotFoundException => {
        log.error("Input file not found")
        Failure(fileNotFoundException)
      }
      case exception: Exception => {
        log.error("Exception found " + exception)
        Failure(exception)
      }
    }
  }
}