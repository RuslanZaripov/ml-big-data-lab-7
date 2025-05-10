import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.DataFrame
import java.util.logging.{Level, Logger}
import java.util.Properties
import java.io.FileInputStream
import scala.collection.JavaConverters._

object Datamart {
  private val logger = Logger.getLogger(getClass.getName)

  private val config_file_path = "conf/spark.ini"
  private val driver = "com.clickhouse.jdbc.ClickHouseDriver"
  private val init_table_name = "openfoodfacts"
  private val proc_table_name = s"${init_table_name}_proc"
  private val usefulCols = Seq(
    "code",
    "fat_100g",
    "carbohydrates_100g",
    "sugars_100g",
    "proteins_100g",
    "salt_100g",
    "sodium_100g"
  )
  private val metadataCols = Seq("code")

  private def loadPropertiesFromFile(
      filePath: String,
      prefix: String
  ): Map[String, String] = {
    val props = new Properties()
    props.load(new FileInputStream(filePath))
    props.asScala.collect {
      case (k, v) if k.startsWith(prefix) => k -> v
    }.toMap
  }

  private val clickhousePairs =
    loadPropertiesFromFile(config_file_path, "clickhouse")

  private val clickhouseIpAddress =
    clickhousePairs.get("clickhouse_ip_address").get
  private val clickhousePort =
    clickhousePairs.get("clickhouse_port").get
  private val clickhouseDatabase =
    clickhousePairs.get("clickhouse_database").get
  private val clickhouseUser =
    clickhousePairs.get("clickhouse_user").get
  private val clickhousePassword =
    clickhousePairs.get("clickhouse_password").get

  private def parseArgs(args: Array[String]): Set[String] = {
    args.map(_.toLowerCase).toSet
  }

  private val url =
    s"jdbc:ch://${clickhouseIpAddress}:${clickhousePort}/${clickhouseDatabase}"

  private def createSparkSession(): SparkSession = {
    val sparkPairs = loadPropertiesFromFile(config_file_path, "spark.")

    var builder: SparkSession.Builder = SparkSession
      .builder()
      .appName("FoodDataClusterizer")

    sparkPairs.foreach { case (k, v) => builder = builder.config(k, v) }

    builder.getOrCreate()
  }

  private def readTable(spark: SparkSession, tableName: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", clickhouseUser)
      .option("password", clickhousePassword)
      .option("query", s"SELECT * FROM $tableName")
      .load()
  }

  private def preprocessData(spark: SparkSession): Unit = {
    logger.info("Starting data preprocessing")

    val df = readTable(spark, init_table_name)

    val featureCols = usefulCols.filterNot(metadataCols.contains)

    val processedDf = df
      .select(usefulCols.map(col): _*)
      .na
      .drop()
      .withColumns(featureCols.map(c => (c, col(c).cast(FloatType))).toMap)

    featureCols.foreach { colName =>
      processedDf.filter(col(colName) < 100)
    }

    featureCols.foreach { colName =>
      processedDf.filter(col(colName) >= 0)
    }

    val linesProcessed = processedDf.count()

    logger.info(s"Processed lines count: $linesProcessed")

    processedDf.write
      .format("jdbc")
      .mode("append")
      .option("driver", driver)
      .option("url", url)
      .option("dbtable", proc_table_name)
      .option("user", clickhouseUser)
      .option("password", clickhousePassword)
      .option("createTableOptions", "ENGINE=MergeTree() ORDER BY code")
      .save()
  }

  private def writeResults(spark: SparkSession): Unit = {
    logger.info("Starting results writing")

    val proc_df = readTable(spark, proc_table_name)
    val init_df = readTable(spark, init_table_name)

    val new_df = init_df
      .drop("prediction")
      .join(proc_df.select("code", "prediction"), "code", "left")

    logger.info(s"joined dataframe row count: ${new_df.count()}")

    new_df.write
      .format("jdbc")
      .mode("append")
      .option("driver", driver)
      .option("url", url)
      .option("dbtable", init_table_name)
      .option("user", clickhouseUser)
      .option("password", clickhousePassword)
      .option("createTableOptions", "ENGINE=MergeTree() ORDER BY code")
      .save()
  }

  def main(args: Array[String]): Unit = {
    logger.info(s"jdbc connection url: $url")
    logger.info(s"creds: user=$clickhouseUser pass=$clickhousePassword")

    val options = parseArgs(args)
    logger.info(s"Options $options")

    val spark = createSparkSession()
    try {
      if (options.contains("preprocess")) {
        preprocessData(spark)
      }
      if (options.contains("write")) {
        writeResults(spark)
      }
    } catch {
      case e: Exception =>
        logger.severe(s"Error in processing: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      logger.info("Stopping Spark session")
      spark.stop()
    }
  }
}
