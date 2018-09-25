package poc.donghang

import java.io.{File, FileInputStream}
import java.util.Properties
import org.apache.log4j.Logger
object FileTool {
  val LOGGER: Logger = Logger.getLogger(getClass)
  //org.apache.log4j.Logger.getLogger("org").setLevel(Level.WARN)

  def loadPropertiesFromPath(propertyFilePath: String): Properties = {
    val props = new Properties()
    try {
      LOGGER.warn("读取" + propertyFilePath + "下的配置文件")
      props.load(new FileInputStream(propertyFilePath))
    } catch {
      case ex: Exception => throw new Exception("Read path : " + propertyFilePath + " caused " + ex)
    }
    props
  }

  def isFileExist(path: String): Boolean = {
    val file: File = new File(path)
    file.exists()
  }
}