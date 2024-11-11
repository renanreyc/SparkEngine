package br.com.company.dataloadhistoryRi


case class Params(
                       master: String= "",
                       sparklogdir: String = "",
                       path: String = "",
                       bucket: String = "",
                       tenant: String = "",
                       year: String = "",
                       month: String = "",
                       bucketconfig: String = "",
                       createTableDelta: String = "",
                       bucketDelta: String = "",
                       singleParquet: Boolean = true
                     ) extends Serializable
object Params {
  def parser(args: Array[String]): Params = {

  new scopt.OptionParser[Params]("Neogrid") {

    opt[String]("master")
      .action((master, params) => {
        params.copy(master = master)
      })
      .text("Set path Obs")

    opt[String]("sparklogdir")
      .action((sparklogdir, params) => {
        params.copy(sparklogdir = sparklogdir)
      })
      .text("Set path Obs")

    opt[String]("path")
      .action((path, params) => {
        params.copy(path = path)
      })
      .text("Set path Obs")

    opt[String]("bucket")
      .action((bucket, params) => {
        params.copy(bucket = bucket)
      })
      .text("Set Bucket name")

    opt[String]("tenant")
      .action((tenant, params) => {
        params.copy(tenant = tenant)
      })
      .text("Set Tenant name to process")

    opt[String]("year")
      .action((year, params) => {
        params.copy(year = year)
      })
      .text("Set Year to process")

    opt[String]("month")
      .action((month, params) => {
        params.copy(month = month)
      })
      .text("Set month to process")

    opt[String]("bucketconfig")
      .action((bucketconfig, params) => {
        params.copy(bucketconfig = bucketconfig)
      })
      .text("Set bucket config")

    opt[String]("createtabledelta")
      .action((createTableDelta, params) => {
        params.copy(createTableDelta = createTableDelta)
      })
      .text("Set create table object in bucket path")

    opt[String]("bucketDelta")
      .action((bucketDelta, params) => {
        params.copy(bucketDelta = bucketDelta)
      })
      .text("Set create table object in bucket path")

    opt[Boolean]("singleParquet")
      .action((singleParquet, params) => {
        params.copy(singleParquet = singleParquet)
      })
      .text("Set if only one parquet per file")

  }.parse(args, Params()) match {
    case Some(params) => params
    case _ => throw new IllegalArgumentException()
  }
  }
}
