package scala.weather.utils.pressure

import java.util.Properties
import scala.io.Source

/**
 * This class contains utility methods related to pressure property file
 *
 * @author - Sandeep
 */
class pressureUtils {


  //Reading property file
  def readPropertyFile(): Properties = {
    var properties: Properties = null
    val url = getClass.getResource("src/main/resources/properties/pressure/pressureconfig.properties")
    if (url != null) {
      val source = Source.fromURL(url)
      properties = new Properties()
      properties.load(source.bufferedReader())
    }
    properties
  }
}