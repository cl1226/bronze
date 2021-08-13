package org.excitinglab.bronze.core.output

import org.apache.spark.sql.{Dataset, Row}
import org.excitinglab.bronze.apis.BaseOutput
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import java.util.Properties
import scala.collection.JavaConverters._

class Mysql extends BaseOutput {

  var config: Config = ConfigFactory.empty()

  override def process(df: Dataset[Row]): Unit = {
    println(s"[INFO] 转换算子 [${config.getString("type")}] properties: ")
    config.entrySet().asScala.foreach(entry => {
      println("\t" + entry.getKey + " = " + entry.getValue)
    })
    val prop = new Properties()
    prop.setProperty("driver", config.getString("driver"))
    prop.setProperty("user", config.getString("user"))
    prop.setProperty("password", config.getString("password"))

    val saveMode = config.getString("saveMode")
    df.write.mode(saveMode).jdbc(config.getString("url"), config.getString("dbtable"), prop)
  }

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = this.config

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def checkConfig(): (Boolean, String) = {
    val requiredOptions = List("url", "driver", "dbtable", "user", "password")
    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }
    if (nonExistsOptions.nonEmpty) {
      (
        false,
        "please specify " + nonExistsOptions
          .map { option =>
            val (name, exists) = option
            "[" + name + "]"
          }
          .mkString(", ") + " as non-empty string"
      )
    } else {
      (true, "")
    }
  }
}
