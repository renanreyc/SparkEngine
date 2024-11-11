package br.com.neogrid.dataload.process

import br.com.neogrid.commons.CustomLogger
import br.com.neogrid.dataload.model.Configuration
import br.com.neogrid.dataload.utils.Parameters
import com.obs.services.ObsClient
import org.apache.spark.sql.SparkSession

/**
 * Object `Process`
 *
 * This object provides methods for loading, transforming, and outputting data using various sources
 * such as local file systems, OBS (Object Storage), and SQL Server. It serves as a centralized
 * controller for data processing tasks defined in a `Configuration` object.
 */
object Process{

  /**
     * Loads data from various sources into Spark temporary views based on the configuration provided.
     *
     * @param jsonroot      The root configuration object containing load instructions.
     * @param spark         The SparkSession object.
     * @param parameters    Additional parameters that influence the data loading process.
     * @param customLogger  CustomLogger object used for logging operations.
     *
     * @throws IllegalArgumentException if the source specified in the load configuration is not recognized.
     *
     * This method processes paths for Parquet and Delta files, loads data from specified sources
     * (e.g., Local, OBS, SQL Server), and creates temporary views in Spark for further processing.
   */
  def dataLoad(jsonroot: Configuration, spark: SparkSession, parameters: Parameters, customLogger: CustomLogger): Unit = {

    if (parameters.pathsPaqruets != null) {
      parameters.pathsPaqruets.foreach(x =>{
        val table = x.keys.mkString("")
        val paths = x.values.mkString("").replace("|", ",").split(",").toList
        val split = paths.head.split("/")
        val bucket = s"${split(0)}//${split(2)}"
        spark.read
          .option("basePath", bucket)
          .parquet(paths: _*)
          .createTempView(table)
      })
    }

    if (parameters.pathsDelta != null) {
      parameters.pathsDelta.foreach(x =>{
        val table = x.keys.mkString("")
        val paths = x.values.mkString("").replace("|", ",").split(",").toList
        spark.read
          .parquet(paths: _*)
          .createTempView(table)
      })
    }

    jsonroot.load.foreach(load => {
      load.source.get.toLowerCase match {
        case "local" => Local.loadFromLocal(spark, load, parameters, customLogger)
        case "obs" => Obs.loadFromObs(spark, load, parameters, customLogger)
        case "sqlserver" => SQLserverDB.loadFromSQLServerDb(spark,load,parameters)
        case _ =>
          var message = s"Fonte não encontrada no load"
          customLogger.logCustomWhithMessage(message, customLogger.returnTypeLevel("ERROR"))
          throw new IllegalArgumentException("Fonte não encontrada no load")
          sys.exit(1)
      }
    }
    )
  }
  /**
   * Transforms data based on the configuration provided, using Spark SQL or other operations.
   *
   * @param jsonroot      The root configuration object containing transformation instructions.
   * @param spark         The SparkSession object.
   * @param parameters    Additional parameters that influence the transformation process.
   * @param customLogger  CustomLogger object used for logging operations.
   *
   * @throws IllegalArgumentException if the source specified in the transformation configuration is not recognized.
   *
   * This method handles data transformations by executing Spark SQL queries on data
   * sourced from local files, OBS, or other sources as defined in the configuration.
   */
  def transformData(jsonroot: Configuration, spark:SparkSession, parameters: Parameters, customLogger: CustomLogger): Unit = {
      jsonroot.transform.foreach(transform =>{
        transform.sourceQuery.get.toLowerCase() match {
          case "local" => Local.transformData(spark, transform, parameters, customLogger)
          case "obs" => Obs.transformDataOBS(spark, transform, parameters, customLogger)
          case _ =>
            var message = s"Fonte não encontrada na transformação"
            customLogger.logCustomWhithMessage(message, customLogger.returnTypeLevel("ERROR"))
            throw new IllegalArgumentException("Fonte mão encontrada na transformação")
            sys.exit(1)
        }
      }
      )
  }
  /**
   * Outputs data from Spark temporary views to various destinations based on the configuration provided.
   *
   * @param jsonroot      The root configuration object containing output instructions.
   * @param spark         The SparkSession object.
   * @param parameters    Additional parameters that influence the data output process.
   * @param customLogger  CustomLogger object used for logging operations.
   *
   * @throws IllegalArgumentException if the destination specified in the output configuration is not recognized.
   *
   * This method writes data to various destinations (e.g., Local, OBS, SQL Server) as specified in the configuration.
   * It also clears Spark's catalog cache after the output operation to free up resources.
   */
  def dataOutput(jsonroot: Configuration, spark:SparkSession, parameters: Parameters, customLogger: CustomLogger): Unit = {

    jsonroot.output.foreach(output => {
      output.source.get.toLowerCase() match {
        case "local" => Local.outPutLocal(spark, output, customLogger)
        case "obs" => Obs.outputObs(spark, output, customLogger)
        case "sqlserver" => SQLserverDB.WriteSQLServerDb(spark, output, parameters)
        case _ =>
          var message = s"Fonte mão encontrada no output"
          customLogger.logCustomWhithMessage(message, customLogger.returnTypeLevel("ERROR"))
          throw new IllegalArgumentException("Fonte mão encontrada no output")
          sys.exit(1)
      }
      spark.catalog.clearCache()
    })
  }




}
