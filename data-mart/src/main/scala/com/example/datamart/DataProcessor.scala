package com.example.datamart

import cats.effect.Sync
import io.circe.{Encoder, Json}
import org.http4s.EntityEncoder
import org.http4s.circe._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.FloatType

trait DataProcessor[F[_]] {
  def processData(
      numPartitions: DataProcessor.NumPartitions
  ): F[DataProcessor.ProcessingResult]
}

object DataProcessor {
  final case class NumPartitions(value: Int) extends AnyVal

  final case class ProcessingResult(
      message: String,
      linesProcessed: Long,
  )

  object ProcessingResult {
    implicit val processingResultEncoder: Encoder[ProcessingResult] =
      new Encoder[ProcessingResult] {
        final def apply(result: ProcessingResult): Json = Json.obj(
          ("message", Json.fromString(result.message)),
          ("linesProcessed", Json.fromLong(result.linesProcessed)),
        )
      }

    implicit def processingResultEntityEncoder[F[_]]
        : EntityEncoder[F, ProcessingResult] =
      jsonEncoderOf[F, ProcessingResult]
  }

  def impl[F[_]: Sync]: DataProcessor[F] = new DataProcessor[F] {
    def processData(numPartitions: NumPartitions): F[ProcessingResult] = {
      Sync[F].delay {
        val config = ConfigLoader
          .loadClickHouseConfig()
          .fold(
            failures => throw new RuntimeException(failures.prettyPrint()),
            identity
          )

        val spark = SparkSession
          .builder()
          .appName("FoodDataClusterizer")
          .config("spark.master", "local[*]")
          .config("spark.driver.memory", "2g")
          .config("spark.executor.memory", "1g")
          .config("spark.executor.instances", "2")
          .config("spark.executor.cores", "2")
          .config("spark.dynamicAllocation.enabled", "true")
          .config("spark.dynamicAllocation.minExecutors", "1")
          .config("spark.dynamicAllocation.maxExecutors", "5")
          .config("spark.sql.execution.arrow.pyspark.enabled", "true")
          .config(
            "spark.jars.packages",
            "com.clickhouse:clickhouse-jdbc:0.4.6,com.clickhouse.spark:clickhouse-spark-runtime-3.4_2.12:0.8.0,com.clickhouse:clickhouse-client:0.7.0,com.clickhouse:clickhouse-http-client:0.7.0,org.apache.httpcomponents.client5:httpclient5:5.2.1"
          )
          .config(
            "spark.sql.catalog.clickhouse",
            "com.clickhouse.spark.ClickHouseCatalog"
          )
          .config(
            "spark.sql.catalog.clickhouse.host",
            config.clickhouseIpAddress
          )
          .config(
            "spark.sql.catalog.clickhouse.protocol",
            config.clickhouseProtocol
          )
          .config(
            "spark.sql.catalog.clickhouse.http_port",
            config.clickhousePort
          )
          .config("spark.sql.catalog.clickhouse.user", config.clickhouseUser)
          .config(
            "spark.sql.catalog.clickhouse.password",
            config.clickhousePassword
          )
          .config(
            "spark.sql.catalog.clickhouse.database",
            config.clickhouseDatabase
          )
          .getOrCreate()

        try {
          val table = "openfoodfacts"
          val indexColName = "numeric_index"
          val numParitions = "11"

          val socketTimeout = 300000
          val url =
            s"jdbc:clickhouse://${config.clickhouseIpAddress}:${config.clickhousePort}/${config.clickhouseDatabase}?socket_timeout=${socketTimeout}"
          val driver = "com.clickhouse.jdbc.ClickHouseDriver"
          val query =
            s"(SELECT *, rowNumberInBlock() AS ${indexColName} FROM ${table}) AS subquery"

          val df = spark.read
            .format("jdbc")
            .option("driver", driver)
            .option("url", url)
            .option("dbtable", query)
            .option("user", config.clickhouseUser)
            .option("password", config.clickhousePassword)
            .option("partitionColumn", indexColName)
            .option("lowerBound", "1")
            .option("upperBound", "100")
            .option("numPartitions", numParitions)
            .option("fetchsize", "10000")
            .load()

          val usefulCols = Seq(
            "code",
            // "energy_kcal_100g",
            "fat_100g",
            "carbohydrates_100g",
            "sugars_100g",
            "proteins_100g",
            "salt_100g",
            "sodium_100g"
          )
          val metadataCols = Seq("code")
          val featureCols = usefulCols.filterNot(metadataCols.contains)

          val processedDf = df
            .select(usefulCols.map(col): _*)
            .na
            .drop()
            .withColumns(
              featureCols.map(c => (c, col(c).cast(FloatType))).toMap
            )
          // .filter(col("energy_kcal_100g") < 1000)

          featureCols.filter(_ != "energy_kcal_100g").foreach { colName =>
            processedDf.filter(col(colName) < 100)
          }

          featureCols.foreach { colName =>
            processedDf.filter(col(colName) >= 0)
          }

          val linesProcessed = processedDf.count()

          ProcessingResult(
            message = "Data processed successfully",
            linesProcessed = linesProcessed,
          )
        } finally {
          spark.stop()
        }
      }
    }
  }
}
