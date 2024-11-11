package br.com.company.dataload.utils

case class Parameters(
                       endpoint: String = "",
                       access: String = "",
                       secret: String = "",
                       masterSpark: String = "",
                       config: String = "",
                       pathsPaqruets: Seq[Map[String, String]] = null,
                       sparklogdir: String = "",
                       tenant: String = "",
                       processId: String = "",
                       table: String = "",
                       nmPartId: String = "",
                       parameters: Seq[String] = null,
                       kafkaTopic: String = "",
                       kafkaKey: String = "",
                       kafkaMsg: String = "",
                       tenantFolderDelta: String = "",
                       kafkaBootstrap: String = "",
                       topicError: String = "",
                       topicSuccess: String = "",
                       pathsDelta: Seq[Map[String, String]] = null,
                       pathsToProcess: Seq[Map[String, String]] = null,
                       bucketPathsToProcess: String = null,
                       JdbcHostname: String = null,
                       JdbcPort: String = null,
                       JdbcCLassDriver: String = null,
                       JdbcUserName: String = null,
                       JdbcPassword: String = null
                     ) extends Serializable
object Parameters {

  lazy val parser = new scopt.OptionParser[Parameters]("Neogrid") {

      opt[String]("endpoint")
        .action((endpoint, params) => {
          params.copy(endpoint = endpoint)
        })
        .text("Set EndPoint Config ")

      opt[String]("access")
        .action((access, params) => {
          params.copy(access = access)
        })
        .text("Set Access Config ")


      opt[String]("secret")
        .action((secret, params) => {
          params.copy(secret = secret)
        })
        .text("Set Secret Config ")


    opt[String]("master")
      .action((masterSpark, params) => {
        params.copy(masterSpark = masterSpark)
      })
      .text("Set master spark")
      .required()


    opt[String]("config")
      .action((config, params) => {
        params.copy(config = config)
      })
      .text("Set config spark")
      .required()

    //----------------
    opt[String]("sparklogdir")
      .action((sparklogdir, params) => {
        params.copy(sparklogdir = sparklogdir)
      })
      .text("Set sparklogdir")

    opt[Seq[Map[String, String]]]("pathsParquet")
      .action((pathsPaqruets, params) => {
        params.copy(pathsPaqruets = pathsPaqruets)
      })
      .text("Set sparklogdir")


    opt[String]("tenant")
      .action((tenant, params) => {
        params.copy(tenant = tenant)
      })
      .text("Set tenant")

    opt[String]("process_id")
      .action((processId, params) => {
        params.copy(processId = processId)
      })
      .text("Set process_id")

    opt[String]("table")
      .action((table, params) => {
        params.copy(table = table)
      })
      .text("Set table")

    opt[String]("nm_part_id")
      .action((nmPartId, params) => {
        params.copy(nmPartId = nmPartId)
      })
      .text("Set nm_part_id")


    opt[Seq[String]]("parameters")
      .action((parameters, params) => {
        params.copy(parameters = parameters)
      })
      .text("Set parameters")


    opt[String]("kafka_topic")
      .action((kafkaTopic, params) => {
        params.copy(kafkaTopic = kafkaTopic)
      })
      .text("Set topic kafka")

    opt[String]("kafka_key_msg")
      .action((kafkaKey, params) => {
        params.copy(kafkaKey = kafkaKey)
      })
      .text("Set key kafka")


    opt[String]("kafka_msg")
      .action((kafkaMsg, params) => {
        params.copy(kafkaMsg = kafkaMsg)
      })
      .text("Set msg kafka")

    opt[String]("kafka_bootstrap")
      .action((kafkaBootstrap, params) => {
        params.copy(kafkaBootstrap = kafkaBootstrap)
      })
      .text("Set bootstrap kafka")


    opt[String]("tenant_folder_delta")
      .action((tenantFolderDelta, params) => {
        params.copy(tenantFolderDelta = tenantFolderDelta)
      })
      .text("Set if tenant folder is necessary")

    opt[String]("kafka_topic_error")
      .action((topicError, params) => {
        params.copy(topicError = topicError)
      })
      .text("Set topic error kafka")


    opt[Seq[Map[String, String]]]("pathsDelta")
      .action((pathsDelta, params) => {
        params.copy(pathsDelta = pathsDelta)
      })
      .text("Set paths delta")

    opt[Seq[Map[String, String]]]("pathsToProcess")
      .action((pathsToProcess, params) => {
        params.copy(pathsToProcess = pathsToProcess)
      })
      .text("Set pathsToProcess")


    opt[String]("bucket_paths_to_process")
      .action((bucketPathsToProcess, params) => {
        params.copy(bucketPathsToProcess = bucketPathsToProcess)
      })
      .text("Set bucket paths to process")

    //----------------- JDBC

    opt[String]("jdbc_host_name")
      .action((JdbcHostname, params) => {
        params.copy(JdbcHostname = JdbcHostname)
      })
      .text("Set Jdbc host name")

    opt[String]("jdbc_port")
      .action((JdbcPort, params) => {
        params.copy(JdbcPort = JdbcPort)
      })
      .text("Set jdbc port")

    opt[String]("jdbc_class_driver")
      .action((JdbcCLassDriver, params) => {
        params.copy(JdbcCLassDriver = JdbcCLassDriver)
      })
      .text("Set jdbc class driver")

    opt[String]("jdbc_username")
      .action((JdbcUserName, params) => {
        params.copy(JdbcUserName = JdbcUserName)
      })
      .text("Set user name")

    opt[String]("jdbc_password")
      .action((JdbcPassword, params) => {
        params.copy(JdbcPassword = JdbcPassword)
      })
      .text("Set jdbc password")
    }

}
