package utilities

import java.io.File
import scala.collection.JavaConverters._
import Constants._
import com.typesafe.config.ConfigException.Missing
import com.typesafe.config.{Config, ConfigFactory, ConfigResolveOptions}

class ConfigurationHelper(baseDir: String = EMPTY, argsProperties: Map[String, String] = null, fileName: String = "") {

  private val configResolutionOptions = ConfigResolveOptions.defaults().setAllowUnresolved(true)
  private val args: Config = if (argsProperties == null) {
    ConfigFactory.empty()
  } else {
    ConfigFactory.load(ConfigFactory.parseMap(argsProperties.asJava))
  }
  val config: Config = fileName match {
    case EMPTY =>
      val default = ConfigFactory.load(ConfigFactory.parseFile(new File(s"$baseDir$DEFAULT_CONFIGURATION")).resolveWith(args, configResolutionOptions), configResolutionOptions)
      args /*.withFallback(application)*/ .withFallback(default)
    case _     =>
      val fileConfig = ConfigFactory.load(ConfigFactory.parseFile(new File(s"$baseDir$fileName")).resolve())
      args.withFallback(fileConfig)
  }

  @throws(classOf[Missing])
  def getString(key: String): String = {
    config.getString(getConfig(key))
  }

  @throws(classOf[Missing])
  private def getConfig(key: String): String = {
    if (hasPathFor(key)) {
      s"$key"
    }
    else
      throw new Missing(key, null)
  }

  private def hasPathFor(key: String): Boolean = {
    config.hasPath(s"$key")
  }
}
