package br.com.company.dataloadhistoryRi

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark {


  def getSession(appname: String, params: Params): SparkSession = {

    lazy val sparkConf = {
      val conf = new SparkConf()
      if (params.master.equals("local"))
        conf.set("spark.master", "local[*]")
      if (!params.sparklogdir.equalsIgnoreCase("")){
        conf.set("spark.eventLog.dir", params.sparklogdir)
        conf.set("spark.eventLog.enabled", "true")
      }
      conf
    }

    lazy val spark = SparkSession
      .builder()
      .appName(appname)
      .config("spark.hadoop.fs.obs.impl", "org.apache.hadoop.fs.obs.OBSFileSystem")
      .config("spark.hadoop.fs.AbstractFileSystem.obs.impl", "org.apache.hadoop.fs.obs.OBS")
      .config("spark.hadoop.fs.obs.endpoint", System.getenv("OBS_ENDPOINT_URL"))
      .config("spark.hadoop.fs.obs.access.key", System.getenv("OBS_ACCESS_KEY_ID"))
      .config("spark.hadoop.fs.obs.secret.key", System.getenv("OBS_SECRET_ACCESS_KEY"))
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.databricks.delta.schema.autoMerge.enabled","true")
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .config(sparkConf)
      .getOrCreate()
    spark
  }

}
