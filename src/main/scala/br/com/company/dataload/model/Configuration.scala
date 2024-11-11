package br.com.company.dataload.model

import org.json4s.JsonAST.JObject
case class Configuration(load: Seq[Load],
                          transform: Seq[Transform],
                          output: Seq[Output]
                        )

case class Load(
               source: Option[String] = None,
               method: Option[String] = None,
               pathFiles: Option[String] = None,
               format: Option[String] = None,
               tempView: Option[String] = None,
               options: Option[JObject] = None,
               pathQuery: Option[String] = None,
               bucket: Option[String] = None,
               createTableLocation: Option[String] = None,
               table: Option[String] = None,
               bucketConfig: Option[String] = None,
               ignoreReadTenantFolder: Option[String],
               JdbcDatabase: Option[String],
               JdbcTableName: Option[String]
               )

case class Transform(
                 sourceQuery: Option[String] = None,
                 tempView: Option[String] = None,
                 pathQuery: Option[String] = None,
                 bucket: Option[String] = None,
                 output: Option[String] = None,
                 nestedQuery: Option[Array[String]] = None
               )

case class Output(
                 source: Option[String] = None,
                 pathOutputFiles: Option[String] = None,
                 mode: Option[String] = None,
                 format: Option[String] = None,
                 options: Option[JObject] = None,
                 tempViewToWrite: Option[String] = None,
                 partitionedBy: Option[Array[String]] = None,
                 bucket: Option[String] = None,
                 JdbcDatabase: Option[String],
                 JdbcTableName: Option[String]
                 )
