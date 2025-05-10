import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.DataFrame
import java.util.logging.{Level, Logger}

object Datamart {
  private val logger = Logger.getLogger(getClass.getName)

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

  private val config = ConfigLoader
    .loadClickHouseConfig()
    .fold(
      failures => throw new RuntimeException(failures.prettyPrint()),
      identity
    )

  private def parseArgs(args: Array[String]): Set[String] = {
    args.map(_.toLowerCase).toSet
  }

  private val url =
    s"jdbc:ch://${config.clickhouseIpAddress}:${config.clickhousePort}/${config.clickhouseDatabase}"

  private def createSparkSession(): SparkSession = {
    SparkSession
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
      .getOrCreate()
  }

  private def readTable(spark: SparkSession, tableName: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", config.clickhouseUser)
      .option("password", config.clickhousePassword)
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
      .option("user", config.clickhouseUser)
      .option("password", config.clickhousePassword)
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
      .option("user", config.clickhouseUser)
      .option("password", config.clickhousePassword)
      .option("createTableOptions", "ENGINE=MergeTree() ORDER BY code")
      .save()
  }

  def main(args: Array[String]): Unit = {
    logger.info(s"Configuration: $config")
    logger.info(s"jdbc connection url: $url")

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
