package br.com.neogrid.dataload.process


import br.com.neogrid.dataload.model.{Load, Output}
import br.com.neogrid.dataload.utils.Functions.{readObjectOBS, replecementsParameters}
import br.com.neogrid.dataload.utils.Parameters
import com.obs.services.ObsClient
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import javax.management.Query


object SQLserverDB {
  /**
   * Loads data from a SQL Server database into a Spark temporary view.
   *
   * @param spark       The SparkSession object.
   * @param load        The Load object containing information about the data to be loaded.
   * @param parameters  The Parameters object containing JDBC connection details.
   *
   * This method establishes a connection to a SQL Server database using the provided JDBC details,
   * reads the specified table, and creates a temporary view in Spark for further processing.
  */
  def loadFromSQLServerDb(spark: SparkSession, load: Load, parameters: Parameters): Unit = {

    //val jdbcHostname = "SQLPRDVISDB04.viveiro.local"
    //val jdbcPort = 1040
    //val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    //val jdbcDatabase = "Masterdata"
    //val jdbcUsername = "insight"
    //val jdbcPassword = "insight"

    val jdbcHostname = parameters.JdbcHostname
    val jdbcPort = parameters.JdbcPort
    val driverClass = parameters.JdbcCLassDriver
    val jdbcDatabase = load.JdbcDatabase.get
    val jdbcUsername = parameters.JdbcUserName
    val jdbcPassword = parameters.JdbcPassword
    val jdbctableName = load.JdbcTableName.get

    val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};databaseName=${jdbcDatabase};encrypt=false;trustServerCertificate=false"

    val connectionProperties = new Properties()
    connectionProperties.put("driver", driverClass)
    connectionProperties.put("user", jdbcUsername)
    connectionProperties.put("password", jdbcPassword)
    connectionProperties.put("fetchsize", "6000000")
    connectionProperties.put("batchsize", "1000000")
    connectionProperties.put("numPartitions", "10")
    connectionProperties.put("autocommit", "true")

    spark.read.jdbc(jdbcUrl, jdbctableName, connectionProperties).createTempView(load.tempView.get)

  }
  /**
   * Writes data from a Spark temporary view to a SQL Server database.
   *
   * @param spark       The SparkSession object.
   * @param output      The Output object containing information about the data to be written.
   * @param parameters  The Parameters object containing JDBC connection details.
   *
   * This method establishes a connection to a SQL Server database using the provided JDBC details,
   * and writes the data from the specified Spark temporary view to the given SQL Server table.
   */
  def WriteSQLServerDb(spark: SparkSession, output: Output, parameters: Parameters): Unit = {

    val jdbcHostname = parameters.JdbcHostname
    val jdbcPort = parameters.JdbcPort
    val driverClass = parameters.JdbcCLassDriver
    val jdbcDatabase = output.JdbcDatabase.get
    val jdbcUsername = parameters.JdbcUserName
    val jdbcPassword = parameters.JdbcPassword
    val jdbctableName = output.JdbcTableName.get

    val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};databaseName=${jdbcDatabase};encrypt=false;trustServerCertificate=false"

    val connectionProperties = new Properties()
    connectionProperties.put("driver", driverClass)
    connectionProperties.put("user", jdbcUsername)
    connectionProperties.put("password", jdbcPassword)
    connectionProperties.put("fetchsize", "60000")
    connectionProperties.put("batchsize", "10000")
    connectionProperties.put("numPartitions", "64")
    connectionProperties.put("autocommit", "true")

    spark.table(output.tempViewToWrite.get)
      .write
      .jdbc(jdbcUrl, jdbctableName, connectionProperties)

  }

}
