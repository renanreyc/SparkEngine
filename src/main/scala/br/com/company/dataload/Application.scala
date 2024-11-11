package br.com.company.dataload


import br.com.company.commons.CustomLogger
import br.com.company.dataload.utils._
import org.apache.spark.sql.SparkSession
import br.com.company.dataload.utils.Functions._
import br.com.company.dataload.utils.Parameters._
import org.apache.log4j.Logger
import br.com.company.dataload.process.Process
import org.apache.commons.lang3.exception.ExceptionUtils
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object Application {

    /**
     * The main entry point for the application.
     *
     * @param args Command-line arguments passed to the application.
     *
     * This method initializes the application by parsing command-line arguments into a Parameters object,
     * and then invokes the `run` method. If parsing fails, the application exits with a status code of 1.
    */
  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger(getClass.getName)


    val defaultParans = Parameters()
    parser.parse(args, defaultParans).map { p =>
      run(p)
    } getOrElse {
      System.exit(1)
    }

    /**
      * Runs the core processing logic of the application.
      *
      * @param parameters The Parameters object containing configuration and runtime options.
      *
      * This method performs the main processing tasks of the application. It initializes Spark, sets up logging,
      * and constructs messages for Kafka based on the processing status. It also handles potential errors,
      * logs them, and sends error messages to a Kafka topic if configured.
    */
    def run(parameters: Parameters): Unit = {

      logger.info("Starting process")
      val customLogger = new CustomLogger

      val spark: SparkSession = SparkBase
        .getSession("App teste", parameters)


      val currentDateTime = LocalDateTime.now()
      val formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")
      val formattedDateTime = currentDateTime.format(formatter)


      var kafkaMessageError = ""
      var kafkaMessageSuccess = ""
      if(!parameters.kafkaTopic.equals("")) {
        kafkaMessageError = s"""{"parameters":"${parameters.parameters}", "hora": ${formattedDateTime}, "status": "Fahou o processamento"}"""
        kafkaMessageSuccess = s"""{"parameters":"${parameters.parameters}", "hora": ${formattedDateTime}, "status": "Terminou com sucesso"}"""
      }


      try {
        customLogger.logCustomWhithMessage("Starting process", customLogger.returnTypeLevel("INFO"))

        val json = readJson(parameters.config, parameters, customLogger)

        Process.dataLoad(json, spark, parameters, customLogger)
        Process.transformData(json, spark, parameters, customLogger)
        Process.dataOutput(json, spark,parameters ,customLogger)

      } catch {
        case e: Throwable => {
          logger.error(ExceptionUtils.getStackTrace(e))
          customLogger.logCustomWhithMessage(ExceptionUtils.getStackTrace(e) , customLogger.returnTypeLevel("ERROR"))
          if (!parameters.topicError.equals("")) sendSingleMsg(parameters.topicError, parameters.kafkaKey, kafkaMessageError, parameters.kafkaBootstrap)
          sys.exit(1)
        }
      }

      if (!parameters.kafkaTopic.equals("")) {
          sendSingleMsg(parameters.kafkaTopic, parameters.kafkaKey, kafkaMessageSuccess, parameters.kafkaBootstrap)
      }
    }
  }
}
