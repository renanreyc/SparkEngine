package br.com.neogrid.dataloadhistoryRi


import br.com.neogrid.commons.CustomLogger
import br.com.neogrid.dataloadhistoryRi.Methods.{createObsClient, createTableDelta, readPathsWriteDelta}
import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.commons.lang3.exception.ExceptionUtils

object Application {
  def main(args: Array[String]): Unit = {

    val parameters = Params.parser(args)

    val logger = Logger.getLogger(getClass.getName)
    val customLogger = new CustomLogger

    val spark: SparkSession = Spark
      .getSession("appname", parameters)

    val obsClient = createObsClient()

    try {
      customLogger.logCustomWhithMessage("Starting process", customLogger.returnTypeLevel("INFO"))
      createTableDelta(parameters, obsClient, spark, customLogger)
      readPathsWriteDelta(parameters,obsClient,spark, customLogger)
    } catch {
      case e: Throwable =>{
        logger.error(ExceptionUtils.getStackTrace(e))
        customLogger.logCustomWhithMessage(ExceptionUtils.getStackTrace(e) , customLogger.returnTypeLevel("ERROR"))
      }
    }
  }
}





