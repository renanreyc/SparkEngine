package br.com.neogrid.dataload.utils

import br.com.neogrid.commons.CustomLogger
import com.obs.services.ObsClient

import java.io.{ByteArrayOutputStream, InputStream}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import java.util.Properties
import java.util.concurrent.ExecutionException
import br.com.neogrid.dataload.model._

import scala.io.Source
import scala.util.{Failure, Success, Try}


object Functions {

  /**
   * Sends a single message to a Kafka topic.
   *
   * @param topicName  The name of the Kafka topic.
   * @param key        The key for the Kafka message.
   * @param msg        The message to be sent.
   * @param server     The Kafka server address.
   *
   * This method configures a Kafka producer with SSL and SASL settings using environment variables,
   * and sends a message to the specified Kafka topic.
   */
  def sendSingleMsg(topicName: String, key: String, msg: String, server: String): Unit = {

    val sslUser = System.getenv("KAFKA_CREDENTIALS_USERNAME")
    val sslPass = System.getenv("KAFKA_CREDENTIALS_PASSWORD")
    val trustLocation = System.getenv("TRUSTSTORE_LOCATION")
    val keyLocation = System.getenv("KEYSTORE_LOCATION")

    val kafkaProducerProps: Properties = {
      val props = new Properties()
      props.put("bootstrap.servers", server)
      props.put("key.serializer", classOf[StringSerializer].getName)
      props.put("value.serializer", classOf[StringSerializer].getName)
      props.put("sasl.mechanism", "PLAIN")
      props.put("security.protocol", "SASL_SSL")
      props.put("ssl.truststore.location", trustLocation)
      props.put("ssl.truststore.password", sslPass)
      props.put("ssl.keystore.location", keyLocation)
      props.put("ssl.keystore.password", sslPass)
      props.put("sasl.jaas.config", s"org.apache.kafka.common.security.plain.PlainLoginModule required username='$sslUser' password='$sslPass';")
      props.put("ssl.endpoint.identification.algorithm", "")
      props
    }
    val producer = new KafkaProducer[String, String](kafkaProducerProps)

    try {
      producer.send(new ProducerRecord[String, String](topicName, key, msg))

    } catch {
      case e: ExecutionException => throw new RuntimeException(e)
        sys.exit(1)
    }

    producer.close()
  }

  /**
   * Reads a JSON configuration file and converts it into a Configuration object.
   *
   * @param config         The path or OBS URI of the JSON configuration file.
   * @param parameters     The Parameters object.
   * @param customLogger   The CustomLogger instance for logging.
   * @return               A Configuration object parsed from the JSON file.
   *
   * This method reads a JSON file either from local storage or OBS, logs the process, and parses it into a Configuration object.
  */
  def readJson(config: String, parameters: Parameters, customLogger: CustomLogger): Configuration = {
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

    val fileContents = if (config.startsWith("obs://")) {
      val obsClient = new ObsClient(System.getenv("OBS_ACCESS_KEY_ID"), System.getenv("OBS_SECRET_ACCESS_KEY"), System.getenv("OBS_ENDPOINT_URL"))
      val obsPattern = """^obs://([^/]+)/(.*)""".r

      config match {
        case obsPattern(bucket, path) =>
          var errorMessage = s"Fazendo a leitura do json no OBS"
          customLogger.logCustomWhithMessage(errorMessage, customLogger.returnTypeLevel("INFO"))
          readObjectOBS(bucket, path, obsClient)
        case _ =>
          var errorMessage = s"Formato OBS inválido."
          customLogger.logCustomWhithMessage(errorMessage, customLogger.returnTypeLevel("ERROR"))
          throw new IllegalArgumentException("Formato OBS inválido.")
          sys.exit(1)
      }
    } else {
      readObjectLocal(config)
    }
    parse(fileContents).extract[Configuration]
  }

  /**
   * Replaces parameters in a SQL query with values from the Parameters object.
   *
   * @param param   The Parameters object containing the replacement values.
   * @param query   The SQL query string where parameters will be replaced.
   * @return        The query string with replaced parameters.
   *
   * This method replaces placeholders in the SQL query with actual values provided in the Parameters object.
  */
  def replecementsParameters(param: Parameters, query: String): String ={
    val replacements = param.parameters.mkString(" ").split(" ").map(_.split("=")).map(arr => arr(0) -> arr(1)).toMap
    val result = replacements.foldLeft(query)((a, b) => a.replaceAllLiterally("$" + b._1, b._2))
    result
  }

  /**
    * Replaces parameters in a SQL query with values from a sequence of strings.
    *
    * @param param   A sequence of strings containing the replacement values.
    * @param query   The SQL query string where parameters will be replaced.
    * @return        The query string with replaced parameters.
    *
    * This method replaces placeholders in the SQL query with actual values provided in the sequence of strings.
  */
  def replecementsParameters2(param: Seq[String], query: String): String ={
    val replacements = param.mkString(" ").split(" ").map(_.split("=")).map(arr => arr(0) -> arr(1)).toMap
    val result = replacements.foldLeft(query)((a, b) => a.replaceAllLiterally("$$" + b._1, b._2))
    result
  }

  /**
   * Reads a file from the local file system.
   *
   * @param path  The path to the file to be read.
   * @return      The contents of the file as a string.
   *
   * This method reads a file from the local file system and returns its contents as a string.
   * If the path is null or empty, or if reading fails, an exception is thrown.
  */
  def readObjectLocal(path: String): String = {
    if (path == null || path.isEmpty) {
      throw new IllegalArgumentException("Caminho do arquivo inválido")
    }
    Try(Source.fromFile(path)) match {
      case Success(file) =>
        try {
          file.mkString
        } finally {
          file.close()
        }
      case Failure(ex) =>
        throw new RuntimeException(s"Falha ao ler o arquivo $path", ex)
        sys.exit(1)
    }
  }

  /**
   * Reads an object from OBS (Object Storage Service).
   *
   * @param bucket      The bucket name in OBS.
   * @param objeto      The object key in OBS.
   * @param obsClient   The ObsClient instance for interacting with OBS.
   * @return            The contents of the object as a string.
   *
   * This method reads an object from OBS using the provided bucket and object key, and returns its contents as a string.
  */
  def readObjectOBS(bucket: String, objeto: String, obsClient: ObsClient): String = {
    val input: InputStream = obsClient.getObject(bucket, objeto).getObjectContent
    val content: String = readInputStreamToString(input)
    input.close()
    content
  }

  /**
   * Converts an InputStream to a string.
   *
   * @param inputStream  The InputStream to be converted.
   * @return             The contents of the InputStream as a string.
   *
   * This method reads the contents of an InputStream and converts it into a string using UTF-8 encoding.
  */
  private def readInputStreamToString(inputStream: InputStream): String = {
    val baos = new ByteArrayOutputStream()
    val buf = new Array[Byte](512)
    var read = 0
    while ({ read = inputStream.read(buf, 0, buf.length); read } != -1) {
      baos.write(buf, 0, read)
    }
    baos.toString("UTF-8")
  }

  /**
   * Extracts input options from the Load object as a map.
   *
   * @param input  The Load object containing input options.
   * @return       A map of input options.
   *
   * This method extracts the input options from the Load object and returns them as a map.
  */
   def readOptionsInput(input: Load): Map[String, String] = {
    var map2 = Map[String, Any]()
    map2 = input.options.get.values ++ map2
    val output = map2.asInstanceOf[Map[String, String]]
    output
  }

  /**
   * Extracts output options from the Output object as a map.
   *
   * @param outputs  The Output object containing output options.
   * @return         A map of output options.
   *
   * This method extracts the output options from the Output object and returns them as a map.
 */
  def readOptionsOutput(outputs: Output): Map[String, String] = {
    var map2 = Map[String, Any]()
    map2 = outputs.options.get.values ++ map2
    val output = map2.asInstanceOf[Map[String, String]]
    output
  }


  /**
   * Constructs a path string based on tenant and partition ID variables.
   *
   * @param parameters  The Parameters object containing path variables.
   * @return            A path string based on the variables.
   *
   * This method constructs and returns a path string based on the tenant and partition ID variables in the Parameters object.
  */
  def setPathVariables(parameters: Parameters): String = {
    if (!parameters.tenant.equalsIgnoreCase("") && parameters.nmPartId.equalsIgnoreCase("")) {
      s"/${parameters.table}/nm_retail=${parameters.tenant}"
    } else if (!parameters.tenant.equalsIgnoreCase("") && !parameters.nmPartId.equalsIgnoreCase("")) {
      s"/${parameters.table}/nm_part_id=${parameters.nmPartId}/nm_retail=${parameters.tenant}/nm_process_id=${parameters.processId}"
    } else {
      ""
    }
  }



}
