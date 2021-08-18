package org.excitinglab.bronze

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.config._
import org.apache.hadoop.fs.Path
import org.excitinglab.bronze.apis.{BaseMl, BaseOutput, BaseStaticInput, BaseTransform, Plugin}
import org.excitinglab.bronze.utils.{AsciiArt, CompressionUtils}

import java.io.File
import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object Bronze extends Logging {

  var viewTableMap: Map[String, String] = Map[String, String]()
  var master: String = _

  def main(args: Array[String]): Unit = {

    CommandLineUtils.parser.parse(args, CommandLineArgs()) match {
      case Some(cmdArgs) => {
        Common.setDeployMode(cmdArgs.deployMode)
        master = cmdArgs.master
        val configFilePath = getConfigFilePath(cmdArgs)

        cmdArgs.testConfig match {
          case true => {
            new ConfigBuilder(configFilePath).checkConfig
            println("config OK !")
          }
          case false => {
            Try(entrypoint(configFilePath)) match {
              case Success(_) => {}
              case Failure(exception) => {
                exception match {
                  case e @ (_: ConfigRuntimeException | _: UserRuntimeException) => Bronze.showConfigError(e)
                  case e: Exception => throw new Exception(e)
                }
              }
            }
          }
        }
      }
      case None =>
    }
  }

  private[bronze] def getConfigFilePath(cmdArgs: CommandLineArgs): String = {
    Common.getDeployMode match {
      case Some(m) => {
        if (m.equals("cluster")) {
          // only keep filename in cluster mode
          new Path(cmdArgs.configFile).getName
        } else {
          cmdArgs.configFile
        }
      }
    }
  }

  private[bronze] def showConfigError(throwable: Throwable): Unit = {
    println("\n\n===============================================================================\n\n")
    val errorMsg = throwable.getMessage
    println("Config Error:\n")
    println("Reason: " + errorMsg + "\n")
    println("\n===============================================================================\n\n\n")
    throw new ConfigRuntimeException(throwable)
  }

  private def entrypoint(configFile: String): Unit = {

    val configBuilder = new ConfigBuilder(configFile)
    println("[INFO] loading SparkConf: ")
    val sparkConf = createSparkConf(configBuilder)
    sparkConf.getAll.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " => " + value)
    })

    StringUtils.isNotBlank(master) match {
      case true => sparkConf.setMaster(master)
      case false =>
    }

    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
    sparkSession.sparkContext.setLogLevel("error")

    val staticInputs = configBuilder.createStaticInputs("batch")
    val streamingInputs = configBuilder.createStreamingInputs("batch")
    val transforms = configBuilder.createTransforms
    val mls = configBuilder.createMls
    val outputs = configBuilder.createOutputs[BaseOutput]("batch")

    baseCheckConfig(staticInputs, streamingInputs, transforms, mls, outputs)

    if (streamingInputs.nonEmpty) {
//      streamingProcessing(sparkSession, configBuilder, staticInputs, streamingInputs, transforms, outputs)
    } else {
      batchProcessing(sparkSession, configBuilder, staticInputs, transforms, mls, outputs)
    }

  }

  /**
   * Batch Processing
   * */
  private def batchProcessing(
                               sparkSession: SparkSession,
                               configBuilder: ConfigBuilder,
                               staticInputs: List[BaseStaticInput],
                               transforms: List[BaseTransform],
                               mls: List[BaseMl],
                               outputs: List[BaseOutput]): Unit = {

    basePrepare(sparkSession, staticInputs, transforms, mls, outputs)

    // let static input register as table for later use if needed
    val headDs = registerInputTempViewWithHead(staticInputs, sparkSession)

    // when you see this ASCII logo, waterdrop is really started.
    showBronzeAsciiLogo()

    if (staticInputs.nonEmpty) {
      var ds = headDs

      for (f <- transforms) {
        // WARN: we do not check whether dataset is empty or not
        // because take(n)(limit in logic plan) do not support pushdown in spark sql
        // To address the limit n problem, we can implemented in datasource code(such as hbase datasource)
        // or just simply do not take(n).
        // if (ds.take(1).length > 0) {
        //  ds = f.process(sparkSession, ds)
        // }

        ds = filterProcess(sparkSession, f, ds)
        registerFilterTempView(f, ds)

      }
      mls.length > 0 match {
        case true => {
          ds = mls(0).process(sparkSession, ds)
        }
        case _ =>
      }
      outputs.foreach(p => {
        outputProcess(sparkSession, p, ds)
      })

      sparkSession.stop()

    } else {
      throw new ConfigRuntimeException("Input must be configured at least once.")
    }
  }

  private[bronze] def showBronzeAsciiLogo(): Unit = {
    AsciiArt.printAsciiArt("Bronze")
  }

  private[bronze] def filterProcess(
                                        sparkSession: SparkSession,
                                        filter: BaseTransform,
                                        ds: Dataset[Row]): Dataset[Row] = {
    val config = filter.getConfig()
    val fromDs = config.hasPath("source_table_name") match {
      case true => {
        val sourceTableName = config.getString("source_table_name")
        sparkSession.read.table(sourceTableName)
      }
      case false => ds
    }

    filter.process(sparkSession, fromDs)
  }

  private[bronze] def outputProcess(sparkSession: SparkSession, output: BaseOutput, ds: Dataset[Row]): Unit = {
    val config = output.getConfig()
    val fromDs = config.hasPath("source_table_name") match {
      case true => {
        val sourceTableName = config.getString("source_table_name")
        sparkSession.read.table(sourceTableName)
      }
      case false => ds
    }

    output.process(fromDs)
  }

  private[bronze] def registerFilterTempView(plugin: Plugin, ds: Dataset[Row]): Unit = {
    val config = plugin.getConfig()
    if (config.hasPath("result_table_name")) {
      val tableName = config.getString("result_table_name")
      registerTempView(tableName, ds)
    }
  }

  /**
   * Return Head Static Input DataSet
   */
  private[bronze] def registerInputTempViewWithHead(
                                                        staticInputs: List[BaseStaticInput],
                                                        sparkSession: SparkSession): Dataset[Row] = {

    if (staticInputs.nonEmpty) {
      val headInput = staticInputs.head
      val ds = headInput.getDataset(sparkSession)
      registerInputTempView(headInput, ds)

      for (input <- staticInputs.slice(1, staticInputs.length)) {

        val ds = input.getDataset(sparkSession)
        registerInputTempView(input, ds)
      }

      ds

    } else {
      throw new ConfigRuntimeException("You must set static input plugin at least once.")
    }
  }

  private[bronze] def registerInputTempView(
                                                staticInputs: List[BaseStaticInput],
                                                sparkSession: SparkSession): Unit = {
    for (input <- staticInputs) {

      val ds = input.getDataset(sparkSession)
      registerInputTempView(input, ds)
    }
  }

  private[bronze] def registerInputTempView(input: BaseStaticInput, ds: Dataset[Row]): Unit = {
    val config = input.getConfig()
    config.hasPath("table_name") || config.hasPath("result_table_name") match {
      case true => {
        val tableName = config.hasPath("table_name") match {
          case true => {
            @deprecated
            val oldTableName = config.getString("table_name")
            oldTableName
          }
          case false => config.getString("result_table_name")
        }
        registerTempView(tableName, ds)
      }

      case false => {
        throw new ConfigRuntimeException(
          "Plugin[" + input.name + "] must be registered as dataset/table, please set \"result_table_name\" config")

      }
    }
  }

  private[bronze] def registerTempView(tableName: String, ds: Dataset[Row]): Unit = {
    viewTableMap.contains(tableName) match {
      case true =>
        throw new ConfigRuntimeException(
          "Detected duplicated Dataset["
            + tableName + "], it seems that you configured result_table_name = \"" + tableName + "\" in multiple static inputs")
      case _ => {
        ds.createOrReplaceTempView(tableName)
        viewTableMap += (tableName -> "")
      }
    }
  }

  private[bronze] def basePrepare(sparkSession: SparkSession, plugins: List[Plugin]*): Unit = {
    for (pluginList <- plugins) {
      for (p <- pluginList) {
        p.prepare(sparkSession)
      }
    }
  }

  private[bronze] def baseCheckConfig(plugins: List[Plugin]*): Unit = {
    var configValid = true
    for (pluginList <- plugins) {
      for (p <- pluginList) {
        val (isValid, msg) = Try(p.checkConfig) match {
          case Success(info) => {
            val (ret, message) = info
            (ret, message)
          }
          case Failure(exception) => (false, exception.getMessage)
        }

        if (!isValid) {
          configValid = false
          printf("Plugin[%s] contains invalid config, error: %s\n", p.name, msg)
        }
      }

      if (!configValid) {
        System.exit(-1) // invalid configuration
      }
    }
    deployModeCheck()
  }

  private[bronze] def deployModeCheck(): Unit = {
    Common.getDeployMode match {
      case Some(m) => {
        if (m.equals("cluster")) {

          logInfo("preparing cluster mode work dir files...")

          // plugins.tar.gz is added in local app temp dir of driver and executors in cluster mode from --files specified in spark-submit
          val workDir = new File(".")
          logWarning("work dir exists: " + workDir.exists() + ", is dir: " + workDir.isDirectory)

          workDir.listFiles().foreach(f => logWarning("\t list file: " + f.getAbsolutePath))

          // decompress plugin dir
          val compressedFile = new File("plugins.tar.gz")

          Try(CompressionUtils.unGzip(compressedFile, workDir)) match {
            case Success(tempFile) => {
              Try(CompressionUtils.unTar(tempFile, workDir)) match {
                case Success(_) => logInfo("succeeded to decompress plugins.tar.gz")
                case Failure(ex) => {
                  logError("failed to decompress plugins.tar.gz", ex)
                  sys.exit(-1)
                }
              }

            }
            case Failure(ex) => {
              logError("failed to decompress plugins.tar.gz", ex)
              sys.exit(-1)
            }
          }
        }
      }
    }
  }

  private[bronze] def createSparkConf(configBuilder: ConfigBuilder): SparkConf = {
    val sparkConf = new SparkConf()

    configBuilder.getSparkConfigs
      .entrySet()
      .foreach(entry => {
        sparkConf.set(entry.getKey, String.valueOf(entry.getValue.unwrapped()))
      })

    sparkConf
  }

}
