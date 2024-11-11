package br.com.company.dataload.process

import br.com.company.commons.CustomLogger
import br.com.company.dataload.model.{Load, Output, Transform}
import br.com.company.dataload.utils.Functions._
import br.com.company.dataload.utils.Parameters
import org.apache.spark.sql._

/**
 * Object `Local`
 *
 * This object provides methods for interacting with local file systems, including loading, outputting,
 * and transforming data using Apache Spark.
 */
object Local extends  Serializable {

  /**
  * Loads data from a local file system into a Spark temporary view.
  *
  * @param spark         The SparkSession object.
  * @param load          The Load configuration object, containing details like file path, format, method, etc.
  * @param parameters    Additional parameters for loading, such as query parameters.
  * @param customLogger  CustomLogger object used for logging operations.
  *
  * @throws IllegalArgumentException if the file format or load method is not supported.
  *
  * This method supports loading data in various formats (CSV, Parquet, JSON, Delta) based on the `load` configuration.
  * Depending on the method specified (`read` or `query`), it reads data directly into a temporary view or executes a
  * query on the data.
  */
  def loadFromLocal(spark:SparkSession, load: Load , parameters: Parameters, customLogger: CustomLogger):Unit= {

    if (load.method.get.equalsIgnoreCase("read")) {

      var message = "Fazendo a leitura do arquivo para data frame: "+load.pathFiles.get
      customLogger.logCustomWhithMessage(message, customLogger.returnTypeLevel("INFO"))

      val dataFrame = load.format.get.toLowerCase match {
        case "csv" => spark.read.options(readOptionsInput(load)).csv(load.pathFiles.get)
        case "parquet" => spark.read.parquet(load.pathFiles.get)
        case "json" => spark.read.options(readOptionsInput(load)).json(load.pathFiles.get)
        case "delta" => spark.read.format("delta").load(load.pathFiles.get)
        case _ =>
          var message = s"Formato de arquivo não suportado"
          customLogger.logCustomWhithMessage(message, customLogger.returnTypeLevel("ERROR"))
          throw new IllegalArgumentException("Formato de arquivo não suportado")
          sys.exit(1)
      }
      dataFrame.createTempView(load.tempView.get)

    } else if (load.method.get.equalsIgnoreCase("query")) {

      var message = "Fazendo a leitura do arquivo para data quey: "+load.pathQuery.get
      customLogger.logCustomWhithMessage(message, customLogger.returnTypeLevel("INFO"))

      val query = readObjectLocal(load.pathQuery.get)
      val result = if (parameters.parameters != null) replecementsParameters(parameters, query) else query
      spark.sql(result)
    } else {
      var message = s"Método de carga não suportado"
      customLogger.logCustomWhithMessage(message, customLogger.returnTypeLevel("ERROR"))
      throw new IllegalArgumentException("Método de carga não suportado")
      sys.exit(1)
    }
  }
/**
  * Writes data from a Spark temporary view to a local file system.
  *
  * @param spark         The SparkSession object.
  * @param output        The Output configuration object, detailing where and how the data should be written.
  * @param customLogger  CustomLogger object used for logging operations.
  *
  * @throws IllegalArgumentException if the file format is not supported.
  *
  * This method supports writing data in various formats (CSV, Parquet, JSON, Delta) based on the `output` configuration.
  * If the data should be partitioned, it can handle partitioned writes as well.
  */
  def outPutLocal(spark: SparkSession, output: Output, customLogger: CustomLogger): Unit = {

    val tableToWrite = spark.table(output.tempViewToWrite.get)
    val outputFilesPath = output.pathOutputFiles.get

    var message = "Iniciando a escrita no path: "+output.pathOutputFiles.get
    customLogger.logCustomWhithMessage(message, customLogger.returnTypeLevel("INFO"))

    output.format.get.toLowerCase match {
      case "csv" =>
        if (output.partitionedBy.isDefined) tableToWrite.write.mode(output.mode.get).partitionBy(output.partitionedBy.get: _*).options(readOptionsOutput(output)).csv(outputFilesPath)
        tableToWrite.write.mode(output.mode.get).options(readOptionsOutput(output)).csv(outputFilesPath)
      case "parquet" =>
        if (output.partitionedBy.isDefined) tableToWrite.write.mode(output.mode.get).partitionBy(output.partitionedBy.get: _*).parquet(outputFilesPath)
        else tableToWrite.write.mode(output.mode.get).parquet(outputFilesPath)
      case "json"  =>
        if (output.partitionedBy.isDefined) tableToWrite.write.mode(output.mode.get).partitionBy(output.partitionedBy.get: _*).options(readOptionsOutput(output)).json(outputFilesPath)
        else tableToWrite.write.mode(output.mode.get).options(readOptionsOutput(output)).json(outputFilesPath)
      case "delta" =>
        if (output.partitionedBy.isDefined) tableToWrite.write.mode(output.mode.get).partitionBy(output.partitionedBy.get: _*).save(outputFilesPath)
        tableToWrite.write.format("delta").mode(output.mode.get).save(output.pathOutputFiles.get)
      case _ =>
        var message = s"Formato de arquivo não suportado"
        customLogger.logCustomWhithMessage(message, customLogger.returnTypeLevel("ERROR"))
        throw new IllegalArgumentException("Formato de arquivo não suportado")
        sys.exit(1)
    }
  }
  /**
   * Transforms data using a sequence of operations defined in the Transform configuration.
   *
   * @param spark         The SparkSession object.
   * @param transform     The Transform configuration object, which includes query paths, output specifications, etc.
   * @param parameters    Additional parameters that may affect the transformation process.
   * @param customLogger  CustomLogger object used for logging operations.
   *
   * @throws IllegalArgumentException if the output type is not recognized.
   *
   * This method reads a query from a local file, executes a series of Spark SQL transformations, and
   * outputs the results in various ways, including creating temporary views or displaying results directly.
  */
  def transformData(spark:SparkSession, transform: Transform, parameters: Parameters, customLogger: CustomLogger): Unit= {
    val query = readObjectLocal(transform.pathQuery.get)
    val result = if (parameters.parameters != null) replecementsParameters(parameters, query) else query

    var message = "Iniciando o processo de tranformação"
    customLogger.logCustomWhithMessage(message, customLogger.returnTypeLevel("INFO"))

    val df = spark.sql(result)
    if (transform.tempView.isDefined) {
      df.createOrReplaceTempView(transform.tempView.get)
    }

    if (transform.output.isDefined) {
      transform.output.get.toLowerCase match {
        case "show" => df.show()
        case "printschema" => df.printSchema()
        case _ =>
          var message = s"Saída não reconhecida"
          customLogger.logCustomWhithMessage(message, customLogger.returnTypeLevel("ERROR"))
          throw new IllegalArgumentException("Saída não reconhecida")
          sys.exit(1)
      }
    }

  }



}
