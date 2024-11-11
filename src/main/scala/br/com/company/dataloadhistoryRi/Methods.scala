package br.com.company.dataloadhistoryRi


import br.com.company.commons.CustomLogger
import br.com.company.dataload.utils.Functions.readObjectOBS
import com.obs.services.ObsClient
import com.obs.services.model.{ListObjectsRequest, ObjectListing}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.functions.{input_file_name, lit, regexp_extract}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object Methods {

  def readPath (spark: SparkSession, path: String): DataFrame ={
    val vl_year = """vl_year=(\d+)"""
    val vl_month = """vl_month=(\d+)"""

    val df = spark.read.parquet(path)
      .withColumn("vl_year", regexp_extract(input_file_name(),vl_year,1).cast("integer"))
      .withColumn("vl_month", regexp_extract(input_file_name(),vl_month,1).cast("integer"))
      .withColumn("cd_update_control", lit(null))
      .withColumn("vl_quantity_sold_not_null", lit(null))
    df
  }

  def generatePaths(params: Params, obsClient: ObsClient, customLogger: CustomLogger): List[String] = {

    val year = params.year
    val month = params.month
    val path = params.path
    val tenant = params.tenant

    val teste = ListBuffer[String]()

    try {
      var request = new ListObjectsRequest(params.bucket)
      request.setMaxKeys(100)
      var result: ObjectListing = null

      do {
        result = obsClient.listObjects(request)
        result.getObjects.forEach(x => {
          val pathObs = x.getObjectKey

          if (pathObs.contains("part-") && pathObs.contains(path) && pathObs.contains("nm_retail=" + tenant) &&
            (year.isEmpty || pathObs.contains("vl_year=" + year)) &&
            (month.isEmpty || pathObs.contains("vl_month=" + month))) {
            teste += s"obs://${params.bucket}/${singleParquet(params, x.getObjectKey)}"
          }
        })

        request.setMarker(result.getNextMarker)
      } while (result.isTruncated)
    } catch {
      case e: Exception =>
        val errorMessage = s"ERROR - Não foi possível ler os paths: ${e.getMessage}"
        customLogger.logCustomWhithMessage(errorMessage, customLogger.returnTypeLevel("ERROR"))
    }
    teste.toList
  }

  def singleParquet(params: Params, path: String): String = {
    val pattern = """(.*\/)(.*\.parquet)""".r

    params.singleParquet match {
      case false => pattern.replaceAllIn(path, m => m.group(1))
      case true => path
    }
  }

  def createObsClient(): ObsClient = {
    val obsAccessKeyId = System.getenv("OBS_ACCESS_KEY_ID")
    val obsSecretAccessKey = System.getenv("OBS_SECRET_ACCESS_KEY")
    val obsEndpointUrl = System.getenv("OBS_ENDPOINT_URL")

    new ObsClient(obsAccessKeyId, obsSecretAccessKey, obsEndpointUrl)
  }


  def createTableDelta(params: Params, obsClient: ObsClient, spark: SparkSession, customLogger: CustomLogger): Unit = {

    try {
      val bucket = params.bucket
      val createTableDelta = params.createTableDelta
      val bucketDelta = params.bucketDelta
      val tenant = params.tenant
      val pathParquet = params.path


      customLogger.logCustomWhithMessage(s"Lendo o sql no bucket obs://$bucket/$createTableDelta", customLogger.returnTypeLevel("INFO"))

      val query = readObjectOBS(params.bucketconfig, createTableDelta, obsClient)

      customLogger.logCustomWhithMessage(s"Executando a query no spark obs://$createTableDelta", customLogger.returnTypeLevel("INFO"))

      spark.sql(s"$query location 'obs://$bucketDelta/retail-insights/delta/rdw/$tenant'")
    } catch {
      case e: Exception =>
        val errorMessage = s"Não foi possível fazer o create table do arquivo ${params.createTableDelta}"
        customLogger.logCustomWhithMessage(errorMessage, customLogger.returnTypeLevel("ERROR"))
        throw new Exception(ExceptionUtils.getStackTrace(e))
    }
  }

    def readPathsWriteDelta(params: Params, obsClient: ObsClient, spark:SparkSession, customLogger: CustomLogger): Unit ={

      try {

        val bucket = params.bucket
        val tenant = params.tenant
        val pathParquet = params.path

        customLogger.logCustomWhithMessage(s"Lendo os paths no bucket obs://$bucket/$pathParquet", customLogger.returnTypeLevel("INFO"))
        val paths = generatePaths(params, obsClient, customLogger).distinct


        paths.foreach { path =>

          customLogger.logCustomWhithMessage(s"Carregando o Dataframe", customLogger.returnTypeLevel("INFO"))
          val dataframe = readPath(spark, path)

          customLogger.logCustomWhithMessage(s"Escrevendo no delta table $tenant", customLogger.returnTypeLevel("INFO"))
          dataframe.write.format("delta").mode("append").save(s"obs://${params.bucketDelta}/retail-insights/delta/rdw/"+params.tenant)

          spark.catalog.clearCache()
        }
      } catch {
        case e: Exception =>
          val errorMessage = s"Falha em escrever no delta table ${params.tenant}"
          customLogger.logCustomWhithMessage(errorMessage, customLogger.returnTypeLevel("ERROR"))
          throw new Exception(ExceptionUtils.getStackTrace(e))
      }
    }
}
