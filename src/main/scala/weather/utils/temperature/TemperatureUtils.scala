package scala.weather.utils.temperature

import java.util.Properties
import scala.io.Source

/**
 * This class contains utility methods related to temperature property file
 *
 * @author - Sandeep
 */
class temperatureUtils {

  //Reading property file
  def readPropertyFile(): Properties = {
    var properties: Properties = null
    val url = getClass.getResource("src/main/resources/properties/temperature/temperatureconfig.properties")
    if (url != null) {
      val source = Source.fromURL(url)
      properties = new Properties()
      properties.load(source.bufferedReader())
    }
    properties
  }
}