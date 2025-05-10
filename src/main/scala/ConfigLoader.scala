import pureconfig._
import pureconfig.generic.auto._
import pureconfig.error.ConfigReaderFailures

final case class ClickHouseConfig(
    clickhouseName: String,
    clickhouseIpAddress: String,
    clickhouseProtocol: String,
    clickhousePort: String,
    clickhouseUser: String,
    clickhousePassword: String,
    clickhouseDatabase: String,
    clickhouseWriteFormat: String
)

final case class AppConfig(
    name: String,
    numPartitions: Int
)

final case class SparkConfig(
    master: String,
    driver: SparkDriverConfig,
    executor: SparkExecutorConfig,
    dynamicAllocation: SparkDynamicAllocation,
    sql: SparkSQLConfig,
    jars: SparkJarsConfig
)

final case class SparkDriverConfig(memory: String)
final case class SparkExecutorConfig(
    memory: String,
    instances: Int,
    cores: Int
)
final case class SparkDynamicAllocation(
    enabled: Boolean,
    minExecutors: Int,
    maxExecutors: Int
)
final case class SparkSQLConfig(
    execution: SparkSQLExecutionConfig,
    catalog: Map[String, String]
)
final case class SparkSQLExecutionConfig(arrow: SparkArrowConfig)
final case class SparkArrowConfig(pyspark: SparkPysparkConfig)
final case class SparkPysparkConfig(enabled: Boolean)
final case class SparkJarsConfig(packages: String)

object ConfigLoader {
  def loadClickHouseConfig(): Either[ConfigReaderFailures, ClickHouseConfig] = {
    val projectDir = System.getProperty("user.dir")
    val confPath = s"$projectDir/application.conf"
    println(s"Configuration path: $confPath")
    ConfigSource.file(confPath).at("clickhouse-config").load[ClickHouseConfig]
  }
}
