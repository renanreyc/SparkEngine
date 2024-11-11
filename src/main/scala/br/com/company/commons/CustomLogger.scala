package br.com.company.commons

import org.apache.logging.log4j.{Level, LogManager, Logger}

class CustomLogger {

  val logger: Logger = LogManager.getLogger(classOf[CustomLogger])

  def logCustomWhithMessage(message:String, customLevel: Level): Unit ={
   logger.log(customLevel, message)
  }

  def returnTypeLevel(level: String): Level ={
    level match {
      case "ERROR" => Level.forName("ERROR", 200)
      case "INFO" => Level.forName("INFO", 400)
    }
  }
}
