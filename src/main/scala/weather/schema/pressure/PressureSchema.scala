package scala.weather.schema.pressure

/**
 * Schema case class for manual Pressure reading
 *
 * @author - Sandeep
 */
class PressureSchema {

    //Manual pressure reading data schema
    case class SchemaGeneral(
      year:             String,
      month:            String,
      day:              String,
      pressure_morning: String,
      pressure_noon:    String,
      pressure_evening: String)

    //Pressure reading schema for year 1938
    case class Schema1938(
      extra:            String,
      year:             String,
      month:            String,
      day:              String,
      pressure_morning: String,
      pressure_noon:    String,
      pressure_evening: String)

    //Pressure reading schema for year 1756
    case class Schema1756(
      year:                                 String,
      month:                                String,
      day:                                  String,
      pressure_morning:                     String,
      barometer_temperature_observations_1: String,
      pressure_noon:                        String,
      barometer_temperature_observations_2: String,
      pressure_evening:                     String,
      barometer_temperature_observations_3: String)

    //Pressure reading schema for year 1859
    case class Schema1859(
      year:                             String,
      month:                            String,
      day:                              String,
      pressure_morning:                 String,
      thermometer_observations_1:       String,
      air_pressure_reduced_to_0_degC_1: String,
      pressure_noon:                    String,
      thermometer_observations_2:       String,
      air_pressure_reduced_to_0_degC_2: String,
      pressure_evening:                 String,
      thermometer_observations_3:       String,
      air_pressure_reduced_to_0_degC_3: String)

  }