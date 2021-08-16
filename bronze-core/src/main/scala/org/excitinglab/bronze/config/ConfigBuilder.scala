package org.excitinglab.bronze.config

import org.excitinglab.bronze.apis.{BaseMl, BaseOutput, BaseStaticInput, BaseStreamingInput, BaseTransform, Plugin}

import java.io.File
import java.util.ServiceLoader
import scala.language.reflectiveCalls
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import util.control.Breaks._

class ConfigBuilder(configFile: String) {

  val config = load()

  def load(): Config = {

    // val configFile = System.getProperty("config.path", "")
    if (configFile == "") {
      throw new ConfigRuntimeException("Please specify config file")
    }

    println("[INFO] Loading config file: " + configFile)

    // variables substitution / variables resolution order:
    // config file --> syste environment --> java properties

    Try({
      val config = ConfigFactory
        .parseFile(new File(configFile))
        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
        .resolveWith(ConfigFactory.systemProperties, ConfigResolveOptions.defaults.setAllowUnresolved(true))

      val options: ConfigRenderOptions = ConfigRenderOptions.concise.setFormatted(true)
      println("[INFO] parsed config file: " + config.root().render(options))

      config
    }) match {
      case Success(conf) => conf
      case Failure(exception) => throw new ConfigRuntimeException(exception)
    }
  }

  /**
   * check if config is valid.
   * */
  def checkConfig: Unit = {
    val sparkConfig = this.getSparkConfigs
    val staticInput = this.createStaticInputs("batch")
    val streamingInputs = this.createStreamingInputs("streaming")
    val outputs = this.createOutputs[BaseOutput]("batch")
    val transforms = this.createTransforms
    val mls = this.createMls
  }

  def getSparkConfigs: Config = {
    config.getConfig("spark")
  }

  def createTransforms: List[BaseTransform] = {

    var transformList = List[BaseTransform]()
    config
      .getConfigList("transform")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "transform")

        val obj = Class
          .forName(className)
          .newInstance()
          .asInstanceOf[BaseTransform]

        obj.setConfig(plugin)

        transformList = transformList :+ obj
      })

    transformList
  }

  def createStreamingInputs(engine: String): List[BaseStreamingInput[Any]] = {

    var inputList = List[BaseStreamingInput[Any]]()
    config
      .getConfigList("input")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "input", engine)

        val obj = Class
          .forName(className)
          .newInstance()

        obj match {
          case inputObject: BaseStreamingInput[Any] => {
            val input = inputObject.asInstanceOf[BaseStreamingInput[Any]]
            input.setConfig(plugin)
            inputList = inputList :+ input
          }
          case _ => // do nothing
        }
      })

    inputList
  }

  def createStaticInputs(engine: String): List[BaseStaticInput] = {

    var inputList = List[BaseStaticInput]()
    config
      .getConfigList("input")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "input", engine)

        val obj = Class
          .forName(className)
          .newInstance()

        obj match {
          case inputObject: BaseStaticInput => {
            val input = inputObject.asInstanceOf[BaseStaticInput]
            input.setConfig(plugin)
            inputList = inputList :+ input
          }
          case _ => // do nothing
        }
      })

    inputList
  }

  def createOutputs[T <: Plugin](engine: String): List[T] = {

    var outputList = List[T]()
    config
      .getConfigList("output")
      .foreach(plugin => {

        val className = engine match {
          case "batch" | "sparkstreaming" =>
            buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "output", "batch")
          case "structuredstreaming" =>
            buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "output", engine)

        }

        val obj = Class
          .forName(className)
          .newInstance()
          .asInstanceOf[T]

        obj.setConfig(plugin)

        outputList = outputList :+ obj
      })

    outputList
  }

  def createMls: List[BaseMl] = {

    var mlList = List[BaseMl]()
    config
      .getConfigList("ml")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "ml")

        val obj = Class
          .forName(className)
          .newInstance()
          .asInstanceOf[BaseMl]

        obj.setConfig(plugin)

        mlList = mlList :+ obj
      })

    mlList
  }

  private def getInputType(name: String, engine: String): String = {
    name match {
      case _ if name.toLowerCase.endsWith("stream") => {
        engine match {
          case "batch" => "sparkstreaming"
          case "structuredstreaming" => "structuredstreaming"
        }
      }
      case _ => "batch"
    }
  }

  /**
   * Get full qualified class name by reflection api, ignore case.
   * */
  private def buildClassFullQualifier(name: String, classType: String): String = {
    buildClassFullQualifier(name, classType, "")
  }

  private def buildClassFullQualifier(name: String, classType: String, engine: String): String = {

    var qualifier = name
    if (qualifier.split("\\.").length == 1) {

      val packageName = classType match {
        case "input" => ConfigBuilder.InputPackage + "." + getInputType(name, engine)
        case "transform" => ConfigBuilder.TransformPackage
        case "ml" => ConfigBuilder.MlPackage
        case "output" => ConfigBuilder.OutputPackage + "." + engine
      }

      val services: Iterable[Plugin] =
        (ServiceLoader load classOf[BaseStaticInput]).asScala ++
          (ServiceLoader load classOf[BaseStreamingInput[Any]]).asScala ++
          (ServiceLoader load classOf[BaseTransform]).asScala ++
          (ServiceLoader load classOf[BaseMl]).asScala ++
          (ServiceLoader load classOf[BaseOutput]).asScala

      var classFound = false
      breakable {
        for (serviceInstance <- services) {
          val clz = serviceInstance.getClass
          // get class name prefixed by package name
          val clzNameLowercase = clz.getName.toLowerCase()
          val qualifierWithPackage = packageName + "." + qualifier
          if (clzNameLowercase == qualifierWithPackage.toLowerCase) {
            qualifier = clz.getName
            classFound = true
            break
          }
        }
      }
    }

    qualifier
  }
}

object ConfigBuilder {

  val PackagePrefix = "org.excitinglab.bronze.core"
  val TransformPackage = PackagePrefix + ".transform"
  val InputPackage = PackagePrefix + ".input"
  val OutputPackage = PackagePrefix + ".output"
  val MlPackage = PackagePrefix + ".ml"

  val PluginNameKey = "plugin_name"
}
