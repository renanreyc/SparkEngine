package br.com.neogrid.dataload.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkBase {

  /**
   * Initializes and returns a SparkSession configured with the specified application name and parameters.
   *
   * @param appname     The name of the Spark application.
   * @param parameters  An instance of the `Parameters` class that contains various configuration settings.
   * @return            A configured `SparkSession` instance.
   *
   * This method creates a SparkSession with several custom configurations:
   * - Sets the master to "local[*]" if the `masterSpark` parameter is "local".
   * - Configures the event log directory and enables event logging if `sparklogdir` is specified.
   * - Configures Spark to work with OBS (Object Storage Service) by setting the necessary OBS endpoint, access key, and secret key.
   * - Enables Delta Lake extensions and auto schema merging.
   * - Sets partition overwrite mode to dynamic and limits the maximum number of records per file.
   *
   * These configurations ensure that the Spark application is ready to interact with OBS, manage data partitions dynamically, and work efficiently with large datasets.
  */
  def getSession(appname: String,parameters: Parameters): SparkSession = {

    lazy val sparkConf = {
      val conf = new SparkConf()
      if (parameters.masterSpark.equals("local"))
        conf.set("spark.master", "local[*]")
      if (!parameters.sparklogdir.equalsIgnoreCase("")){
          conf.set("spark.eventLog.dir", parameters.sparklogdir)
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
            .config("spark.sql.files.maxRecordsPerFile", "1000000")
      .config(sparkConf)
      .getOrCreate()
    spark
  }

}
