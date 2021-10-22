package scala.weather.schema.temperature

/**
 * Schema case class for manual temperature reading
 *
 * @author - Sandeep
 */

class TemperatureSchema {

    //Manual temperature reading data schema
    case class ManualSchema(
      year:                 String,
      month:                String,
      day:                  String,
      morning:              String,
      noon:                 String,
      evening:              String,
      minimum:              String,
      maximum:              String,
      estimatedDiurnalMean: String)

    //Automatic temperature reading data schema
    case class AutomaticSchema(
      year:                 String,
      month:                String,
      day:                  String,
      morning:              String,
      noon:                 String,
      evening:              String,
      minimum:              String,
      maximum:              String,
      estimatedDiurnalMean: String)

    /*
     * In input data, there is a leading space before the first column.
     * In order to cleanse those data, we use below schema.
     */
    case class UncleanSchema(
      extra:   String,
      year:    String,
      month:   String,
      day:     String,
      morning: String,
      noon:    String,
      evening: String)

    //Actual temperature schema
    case class ActualSchema(
      year:    String,
      month:   String,
      day:     String,
      morning: String,
      noon:    String,
      evening: String)
  
  }