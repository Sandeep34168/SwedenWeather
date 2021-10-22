package scala.spark.pressure

import java.io.FileNotFoundException
import scala.util.Failure
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import scala.weather.utils.pressure
import scala.spark.utilities.SparkConfigUtility
import spark.sql
import scala.weather.schema.pressure.PressureSchema.SchemaGeneral
import scala.weather.schema.pressure.PressureSchema.Schema1756
import scala.weather.schema.pressure.PressureSchema.Schema1859
import scala.weather.schema.pressure.PressureSchema.Schema1938


/**
 * Spark program that transforms pressure data and stores it in hive table.
 *
 * @author - Sandeep
 */

  Class PressureSpark extends SparkConfigUtility {

  // logger
  val log = Logger.getLogger(getClass.getName)

  /*
   * Pressure weather data transformation and analysis
   *
   * @param sparkSession
   */
  def pressureAnalysis(sparkSession: SparkSession): Unit = {

    // Reading property file
    val pressUtils = new pressureUtils()
    val properties = pressUtils.readPropertyFile()

    try {

      //------------------------------Manual Station Pressure Data----------------------------------

      //Reading input data
      val manualPressureDataRDD = sparkSession
        .sparkContext
        .textFile(properties.getProperty("pressure.manual.input.dir"))
        .map(item => item.split("\\s+"))
        .map(column => GeneralSchema(column(0), column(1), column(2), column(3), column(4), column(5)))

      //creating dataframe and adding necessary columns
      val manualPressureDataTempDF = sparkSession.createDataFrame(manualPressureDataRDD)
      val manualPressureSchemaDF = manualPressureDataTempDF
        .withColumn("station", lit("manual"))
        .withColumn("pressure_unit", lit("hpa"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))

      //selecting the required columns in dataframe
      val manualPressureDataDF = manualPressureSchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")


      //----------------------Automatic Station Pressure Data---------------------------------

      //Reading input data
      val automaticPressureRDD = sparkSession.sparkContext
        .textFile(properties.getProperty("pressure.automatic.input.dir"))
        .map(item => item.split("\\s+"))
        .map(column => GeneralSchema(column(0), column(1), column(2), column(3), column(4), column(5)))

      //creating dataframe and adding necessary columns
      val automaticPressureTempDF = sparkSession.createDataFrame(automaticPressureRDD)
      val automaticPressureSchemaDF = automaticPressureTempDF
        .withColumn("station", lit("Automatic"))
        .withColumn("pressure_unit", lit("hpa"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))

      //selecting the required columns in the dataframe
      val automaticPressureDataDF = automaticPressureSchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")


      //-------------------------------Pressure Data (1756)--------------------------------------------

      //Reading input data
      val pressureData1756RDD = sparkSession.sparkContext
        .textFile(properties.getProperty("pressure.1756.input.dir"))
        .map(x => x.split("\\s+"))
        .map(x => Schema1756(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))

      //creating dataframe and adding necessary columns
      val pressureData1756TempDF = sparkSession.createDataFrame(pressureData1756RDD)
      val pressureData1756SchemaDF = pressureData1756TempDF
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("Swedish inches (29.69 mm)"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))

      //selecting the required columns in the dataframe
      val pressureData1756DF = pressureData1756SchemaDF.select(
         "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
         "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
         "barometer_temperature_observations_3", "thermometer_observations_1",
         "thermometer_observations_2", "thermometer_observations_3",
         "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")


     //-------------------------------Pressure Data (1859)--------------------------------------------

      //Reading input data
      val pressureData1859RDD = sparkSession.sparkContext
        .textFile(properties.getProperty("pressure.1859.input.dir"))
        .map(x => x.split("\\s+"))
        .map(x => Schema1859(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11)))

      //creating dataframe and adding necessary columns
      val pressureData1859TempDF = sparkSession.createDataFrame(pressureData1859RDD)
      val pressureData1859SchemaDF = pressureData1859TempDF
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("0.1*Swedish inches (2.969 mm)"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))

      //selecting the required columns in the dataframe
      val pressureData1859DF = pressureData1859SchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")


      //---------------------------------Pressure Data (1862)----------------------------

      //Reading input data
      val pressureData1862RDD = sparkSession.sparkContext
        .textFile(properties.getProperty("pressure.1862.input.dir"))
        .map(x => x.split("\\s+"))
        .map(x => Schema1938(x(0), x(1), x(2), x(3), x(4), x(5), x(6)))

      //creating dataframe and adding necessary columns
      val pressureData1862TempDF = sparkSession.createDataFrame(pressureData1862RDD)
      val pressureData1862SchemaDF = pressureData1862TempDF
        .drop("extra")
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("mhg"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))

      //selecting the required columns in the dataframe
      val pressureData1862DF = pressureData1862SchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")


     //------------------------------Pressure Data (1938)------------------------------------

      //Reading input data
      val pressureData1938RDD = sparkSession.sparkContext
        .textFile(properties.getProperty("pressure.1938.input.dir"))
        .map(item => item.split("\\s+"))
        .map(x => Schema1938(x(0), x(1), x(2), x(3), x(4), x(5), x(6)))

      //creating dataframe and adding necessary columns
      val pressureData1938TempDF = sparkSession.createDataFrame(pressureData1938RDD)
      val pressureData1938SchemaDF = pressureData1938TempDF
        .drop("extra")
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("hpa"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))

      //selecting the required columns in the dataframe
      val pressureData1938DF = pressureData1938SchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")


      //----------------------------------Pressure Data (1961)------------------------------------

      //Reading input data
      val pressureData1961RDD = sparkSession.sparkContext
        .textFile(properties.getProperty("pressure.1961.input.dir"))
        .map(x => x.split("\\s+"))
        .map(x => GeneralSchema(x(0), x(1), x(2), x(3), x(4), x(5)))

      //creating dataframe and adding necessary columns
      val pressureData1961TempDF = sparkSession.createDataFrame(pressureData1961RDD)
      val pressureData1961SchemaDF = pressureData1961TempDF
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("hpa"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))

      //selecting the required columns in the dataframe
      val pressureData1961DF = pressureData1961SchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")

      // Final transformed pressure data
      val pressureDF = manualPressureDataDF
        .union(automaticPressureDataDF)
        .union(pressureData1756DF)
        .union(pressureData1859DF)
        .union(pressureData1862DF)
        .union(pressureData1938DF)
        .union(pressureData1961DF)


      //----------------------------Save pressure data to hive table---------------------------------

      //creating hive table
      sql("""CREATE TABLE PressureData(
            year String, 
            month String, 
            day String, 
            pressure_morning String, 
            pressure_noon String, 
            pressure_evening String, 
            station String, 
            pressure_unit String, 
            barometer_temperature_observations_1 String, 
            barometer_temperature_observations_2 String, 
            barometer_temperature_observations_3 String, 
            thermometer_observations_1 String, 
            thermometer_observations_2 String, 
            thermometer_observations_3 String, 
            air_pressure_reduced_to_0_degC_1 String, 
            air_pressure_reduced_to_0_degC_2 String, 
            air_pressure_reduced_to_0_degC_3 String) 
          STORED AS PARQUET""")

      //writing to hive table created above
      pressureDF.write.mode(SaveMode.Overwrite).saveAsTable("PressureData")


      //---------------------------------------Data Analysis-----------------------------------------

      val totalInputCount = manualPressureDataRDD.count() +
        automaticPressureRDD.count() +
        pressureData1938RDD.count() +
        pressureData1862RDD.count() +
        pressureData1756RDD.count() +
        pressureData1859RDD.count() +
        pressureData1961RDD.count()

      log.info("Input data count is " + totalInputCount)
      log.info("Transformed input data count is " + pressureDF.count())
      log.info("Hive data count " + sql("SELECT count(*) as count FROM PressureData").show(false))

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