package org.excitinglab.bronze.config

import org.excitinglab.bronze.apis.{BaseModel, BaseOutput, BaseStaticInput, BaseStreamingInput, BaseTrain, BaseTransform, BaseValidate, Plugin}

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
    val trains = this.createTrains
    val models = this.createModels
    val validates = this.createValidates
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

  def createTrains: List[BaseTrain] = {

    var trainList = List[BaseTrain]()
    config
      .getConfigList("train")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "train")

        val obj = Class
          .forName(className)
          .newInstance()
          .asInstanceOf[BaseTrain]

        obj.setConfig(plugin)

        trainList = trainList :+ obj
      })

    trainList
  }

  def createValidates: List[BaseValidate] = {

    var validateList = List[BaseValidate]()
    config
      .getConfigList("validate")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "validate")

        val obj = Class
          .forName(className)
          .newInstance()
          .asInstanceOf[BaseValidate]

        obj.setConfig(plugin)

        validateList = validateList :+ obj
      })

    validateList
  }

  def createModels: List[BaseModel] = {

    var modelList = List[BaseModel]()
    config
      .getConfigList("model")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "model")

        val obj = Class
          .forName(className)
          .newInstance()
          .asInstanceOf[BaseModel]

        obj.setConfig(plugin)

        modelList = modelList :+ obj
      })

    modelList
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

  private def getTrainType(name: String): String = {
    name match {
      case _ if name.toLowerCase.endsWith("classifier") => "classification"
      case _ if name.toLowerCase.endsWith("regression") => "regression"
      case _ if name.toLowerCase.endsWith("regressor") => "regression"
      case _ if name.toLowerCase.endsWith("cluster") => "clustering"
      case _ if name.toLowerCase.endsWith("recommendation") => "recommendation"
      case _ => ""
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
        case "train" => ConfigBuilder.TrainPackage + "." + getTrainType(name)
        case "model" => ConfigBuilder.ModelPackage
        case "validate" => ConfigBuilder.ValidatePackage
        case "output" => ConfigBuilder.OutputPackage + "." + engine
      }

      val services: Iterable[Plugin] =
        (ServiceLoader load classOf[BaseStaticInput]).asScala ++
          (ServiceLoader load classOf[BaseStreamingInput[Any]]).asScala ++
          (ServiceLoader load classOf[BaseTransform]).asScala ++
          (ServiceLoader load classOf[BaseTrain]).asScala ++
          (ServiceLoader load classOf[BaseModel]).asScala ++
          (ServiceLoader load classOf[BaseValidate]).asScala ++
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
  val TrainPackage = PackagePrefix + ".train"
  val ModelPackage = PackagePrefix + ".model"
  val ValidatePackage = PackagePrefix + ".validate"

  val PluginNameKey = "plugin_name"
}
