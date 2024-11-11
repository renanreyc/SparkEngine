package br.com.company.dataload.process

import br.com.company.commons.CustomLogger
import br.com.company.dataload.model.{Load, Output, Transform}
import br.com.company.dataload.utils.Functions._
import br.com.company.dataload.utils.Parameters
import com.obs.services._
import com.obs.services.model.ListObjectsRequest
import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex

/**
 * Object `Obs`
 *
 * This object provides methods for interacting with an OBS (Object-Based Storage) system, including loading, outputting,
 * and transforming data using Apache Spark.
 */
object Obs {


  /**
   * Loads data from an OBS location into a Spark temporary view.
   *
   * @param spark         The SparkSession object.
   * @param load          The Load configuration object, containing details like bucket name, path, file format, etc.
   * @param parameters    Additional parameters for loading, such as tenant folder information.
   * @param customLogger  CustomLogger object used for logging operations.
   *
   * @throws IllegalArgumentException if the file format or load method is not supported.
   *
   * This method supports loading data in various formats (CSV, Parquet, JSON, Delta) based on the `load` configuration.
   * Depending on the method specified (`read` or `query`), it reads data directly into a temporary view or executes a
   * query on the data.
   */
  def loadFromObs(spark:SparkSession, load: Load, parameters: Parameters, customLogger: CustomLogger ):Unit= {

    val obsClient = new ObsClient(System.getenv("OBS_ACCESS_KEY_ID"), System.getenv("OBS_SECRET_ACCESS_KEY"), System.getenv("OBS_ENDPOINT_URL"))
    var pathfiles = "obs://"+load.bucket.get+"/"+load.pathFiles.get+s"${if (load.method.get.equalsIgnoreCase("read")) setPathVariables(parameters) else ""}"
    var pathDelta = "obs://"+load.bucket.get+"/"+load.pathFiles.get

    var message2 = "Fazendo leitura do arquivo / path: " + pathfiles
    customLogger.logCustomWhithMessage(message2, customLogger.returnTypeLevel("INFO"))

    if (load.method.get.equalsIgnoreCase("read")) {

      var addtenant = false
      if (load.ignoreReadTenantFolder.isEmpty) {
        addtenant = false
      }
      else {
        addtenant = load.ignoreReadTenantFolder.get.toBoolean
      }

      var tenantFolder = ""
      if (parameters.tenantFolderDelta != "" & !addtenant) {
        tenantFolder = "/" + parameters.tenantFolderDelta
      } else {
        tenantFolder = ""
      }

      val dataFrame = load.format.get.toLowerCase match {
        case "csv" => spark.read.options(readOptionsInput(load)).csv(pathfiles)
        case "parquet" => spark.read.parquet(pathfiles)
        case "json" => spark.read.options(readOptionsInput(load)).json(pathfiles)
        case "delta" => spark.read.format("parquet").load(pathDelta+"/"+load.table.get+tenantFolder)
        case _ =>
          val message = s"Formato de arquivo não suportado"
          customLogger.logCustomWhithMessage(message, customLogger.returnTypeLevel("ERROR"))
          throw new IllegalArgumentException("Formato de arquivo não suportado")
          sys.exit(1)
      }
      dataFrame.createTempView(load.tempView.get)

    } else if (load.method.get.equalsIgnoreCase("query")) {

      var message = "Fazendo a leitura do arquivo para query: "+load.pathFiles.get
      customLogger.logCustomWhithMessage(message, customLogger.returnTypeLevel("INFO"))

      val query = readObjectOBS(load.bucketConfig.get, load.pathFiles.get, obsClient)
      var result = if (parameters.parameters != null) replecementsParameters(parameters, query) else query

      if (result.toLowerCase.contains("create table")) {
        pathDelta = "obs://"+load.bucket.get+"/"+load.createTableLocation.get+"/"+load.table.get+s"${if (parameters.tenantFolderDelta != "") "/"+parameters.tenantFolderDelta else ""}"
        spark.sql(result+s" location '${pathDelta}'")
      } else {
        spark.sql(result)
      }

    } else  {
      var message = s"Método de carga não suportado"
      customLogger.logCustomWhithMessage(message, customLogger.returnTypeLevel("ERROR"))
      throw new IllegalArgumentException("Método de carga não suportado")
      sys.exit(1)
    }
  }


  /**
   * Writes data from a Spark temporary view to an OBS location.
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
  def outputObs(spark: SparkSession, output: Output, customLogger: CustomLogger): Unit = {

    var pathfiles = "obs://"+output.bucket.get+"/"+output.pathOutputFiles.get
    val tableToWrite = spark.table(output.tempViewToWrite.get)

    var message = "Iniciando a escrita no path: "+output.pathOutputFiles.get
    customLogger.logCustomWhithMessage(message, customLogger.returnTypeLevel("INFO"))

    output.format.get.toLowerCase match {
      case "csv" =>
        if (output.partitionedBy.isDefined) tableToWrite.write.mode(output.mode.get).partitionBy(output.partitionedBy.get: _*).options(readOptionsOutput(output)).csv(pathfiles)
        tableToWrite.write.mode(output.mode.get).options(readOptionsOutput(output)).csv(pathfiles)
      case "parquet" =>
        if (output.partitionedBy.isDefined) tableToWrite.write.mode(output.mode.get).partitionBy(output.partitionedBy.get: _*).parquet(pathfiles)
        else tableToWrite.write.mode(output.mode.get).parquet(pathfiles)
      case "json"  =>
        if (output.partitionedBy.isDefined) tableToWrite.write.mode(output.mode.get).partitionBy(output.partitionedBy.get: _*).options(readOptionsOutput(output)).json(pathfiles)
        else tableToWrite.write.mode(output.mode.get).options(readOptionsOutput(output)).json(pathfiles)
      case "delta" =>
        if (output.partitionedBy.isDefined) tableToWrite.write.mode(output.mode.get).partitionBy(output.partitionedBy.get: _*).save(pathfiles)
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
  * This method reads a query from OBS, optionally processes multiple paths, and executes a series of Spark SQL
  * transformations. The results can be outputted to a temporary view or shown on the console.
  */
  def transformDataOBS(spark:SparkSession, transform: Transform, parameters: Parameters, customLogger: CustomLogger): Unit= {
    val obsClient = new ObsClient(System.getenv("OBS_ACCESS_KEY_ID"), System.getenv("OBS_SECRET_ACCESS_KEY"), System.getenv("OBS_ENDPOINT_URL"))
    val query = readObjectOBS(transform.bucket.get, transform.pathQuery.get, obsClient)
    val result = if (parameters.parameters != null) replecementsParameters(parameters, query) else query

    var message = "Iniciando o processo de tranformação: "+transform.pathQuery.get
    customLogger.logCustomWhithMessage(message, customLogger.returnTypeLevel("INFO"))

    if (parameters.pathsToProcess != null) {
      parameters.pathsToProcess.foreach(x => {
        val paths = x.values.mkString("").replace("|", ",").split(",").toList

        paths.foreach(path => {

          val formatPath = path.replace(">","=")
          val bucket = parameters.bucketPathsToProcess
          val listObjectsRequest = new ListObjectsRequest(bucket)
          listObjectsRequest.setPrefix(formatPath)
          listObjectsRequest.setMaxKeys(1)
          val exists = obsClient.listObjects(listObjectsRequest).getObjects.size() > 0

          if (!exists) {
            println("Path não existe: "+formatPath)
          } else {

            val pattern: Regex = "vl_year=(\\d+)/vl_month=(\\d+)/".r
            val result = pattern.findFirstMatchIn(formatPath)
            val (ano, mes) = result match {
              case Some(m) => (m.group(1), m.group(2))
              case None => ("", "")
            }

            val temp = "temp"+ano+mes
            val createresult = "result"+ano+mes

            spark.read.parquet("obs://"+bucket+"/"+formatPath).createOrReplaceTempView(temp)

            val readQueryCreate = readObjectOBS(transform.bucket.get, transform.pathQuery.get, obsClient)
            val resultQueryCreate = replecementsParameters2(Seq(s"temp_fact=$temp"), readQueryCreate)
            val resultQueryCreateResult = replecementsParameters2(Seq(s"result_fact=$createresult"), resultQueryCreate)
            val resultQueryCreateRep = replecementsParameters(parameters, resultQueryCreateResult)
            spark.sql(resultQueryCreateRep)

            transform.nestedQuery.get.foreach(query =>{
              val readQueryExecute = readObjectOBS(transform.bucket.get, query, obsClient)
              val resultQueryExecute = replecementsParameters2(Seq(s"temp_fact=$temp"), readQueryExecute)
              val resultQueryExecuteResult = replecementsParameters2(Seq(s"result_fact=$createresult"), resultQueryExecute)
              val resultQueryExecuteRep = replecementsParameters(parameters, resultQueryExecuteResult)
              spark.sql(resultQueryExecuteRep)
            })
          }
        })
      })

    } else {

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
            throw new IllegalArgumentException("Output não suportado")
            sys.exit(1)
        }
      }
    }
  }

}
