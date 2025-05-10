import os
import sys
import argparse
import configparser
from logger import Logger
from functools import reduce
from dotenv import load_dotenv

sys.path.append(os.environ['SPARK_HOME'] + '/python')
sys.path.append(os.environ['SPARK_HOME']+ '/python/build')
sys.path.append(os.environ['SPARK_HOME'] + '/python/pyspark')
sys.path.append(os.environ['SPARK_HOME'] + '/python/lib/py4j-0.10.9.7-src.zip')

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.types import FloatType

SHOW_LOG = True


class Clusterizer():
    def __init__(self, numParitions: str):
        logger = Logger(SHOW_LOG)
        self.config = configparser.ConfigParser()
        self.log = logger.get_logger(__name__)
        
        load_dotenv()
        
        IP_ADDRESS = os.environ["CLICKHOUSE_IP_ADDRESS"]
        PORT = os.environ["CLICKHOUSE_PORT"]
        self.USER = os.environ["CLICKHOUSE_USER"]
        self.PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]
        self.DATABASE = os.environ["CLICKHOUSE_DATABASE"]
        PROTOCOL = os.environ["CLICKHOUSE_PROTOCOL"]
        self.TABLE = "openfoodfacts_proc"
        spark_config_apth = 'conf/spark.ini'
        self.numParitions = numParitions # from 1 to 100
        socket_timeout = 300000
        
        self.useful_cols = [
            'code',
            'fat_100g',
            'carbohydrates_100g',
            'sugars_100g',
            'proteins_100g',
            'salt_100g',
            'sodium_100g',
        ]
        self.metadata_cols = ['code']
        self.feature_cols = [c for c in self.useful_cols if c not in self.metadata_cols]
        
        self.config = configparser.ConfigParser()
        self.config.optionxform = str
        self.config.read(spark_config_apth)
        
        conf = SparkConf()
        config_params = list(self.config['spark'].items())
        conf.setAll(config_params)
        
        params_str = '\n'.join([f'{k}: {v}' for k, v in config_params])
        self.log.info(f"Spark App Configuration Params:\n{params_str}")
            
        self.spark = SparkSession.builder.config(conf=conf) \
            .getOrCreate()
            # .config("spark.sql.catalog.clickhouse.host", IP_ADDRESS) \
            # .config("spark.sql.catalog.clickhouse.protocol", PROTOCOL) \
            # .config("spark.sql.catalog.clickhouse.http_port", PORT) \
            # .config("spark.sql.catalog.clickhouse.user", self.USER) \
            # .config("spark.sql.catalog.clickhouse.password", self.PASSWORD) \
            # .config("spark.sql.catalog.clickhouse.database", self.DATABASE) \
            
        self.url = f"jdbc:ch://{IP_ADDRESS}:{PORT}/{self.DATABASE}?socket_timeout={socket_timeout}"
        self.log.info(f"jdbc connection url: {self.url}")
        self.driver = "com.clickhouse.jdbc.ClickHouseDriver"
        # self.index_col_name = "numeric_index"
        # self.query = f"""(SELECT *, rowNumberInBlock() AS {self.index_col_name} FROM {self.TABLE}) AS subquery"""
        self.query = f'select * from {self.TABLE}'
        
    def cluster(self, cluster_df):
        cluster_count = int(self.config['model']['k'])
        seed = int(self.config['model']['seed'])

        kmeans = KMeans(k=cluster_count).setSeed(seed)
        kmeans_model = kmeans.fit(cluster_df)
        
        return kmeans_model
    
    def evaluate(self, cluster_df):
        evaluator = ClusteringEvaluator(metricName="silhouette", distanceMeasure="squaredEuclidean")
        score = evaluator.evaluate(cluster_df)
        return score
    
    def save_results(self, transformed):
        # self.spark.sql(f'TRUNCATE TABLE clickhouse.{self.DATABASE}.{self.TABLE}')
        transformed.select(*self.useful_cols, 'prediction').write \
            .format("jdbc") \
            .option("driver", self.driver) \
            .option("url", self.url) \
            .option("dbtable", self.TABLE) \
            .option("user", self.USER) \
            .option("password", self.PASSWORD) \
            .option("createTableOptions", "ENGINE=MergeTree() ORDER BY code") \
            .mode("overwrite") \
            .save()
        
    
    def run(self):
        df = self.spark.read \
            .format('jdbc') \
            .option('driver', self.driver) \
            .option('url', self.url) \
            .option('user', self.USER) \
            .option('password', self.PASSWORD) \
            .option('query', self.query) \
            .load()
            # .option("partitionColumn", self.index_col_name) \
            # .option("lowerBound", "1") \
            # .option("upperBound", "100") \
            # .option("numPartitions", self.numParitions) \
            # .option("fetchsize","10000") \
  
        df.cache()
            
        self.log.info(f"{df.count()} loaded lines count")
        
        cluster_df = VectorAssembler(
            inputCols=self.feature_cols, 
            outputCol="features"
        ).transform(df)
        
        model = self.cluster(cluster_df)
        self.log.info(f"Class distribution: {model.summary.clusterSizes}")
        
        pred_df = model.transform(cluster_df)
        
        score = self.evaluate(pred_df)
        self.log.info(f"Silhouette Score: {score}")
         
        self.save_results(pred_df)
        self.log.info('Results saved successfully!')
        
        self.log.info('Stopping spark app...')
        self.spark.stop()


def validate_partitions(value):
    try:
        ivalue = int(value)
        if not 1 <= ivalue <= 100:
            raise argparse.ArgumentTypeError("numPartitions must be between 1 and 100")
        return value
    except ValueError:
        raise argparse.ArgumentTypeError("numPartitions must be a numeric value")


def parse_arguments():
    parser = argparse.ArgumentParser(description='Clusterize food products data using KMeans')
    parser.add_argument(
        '--numPartitions',
        type=validate_partitions,
        required=True,
        help='Number of partitions for data processing (from 1 to 100)'
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    clusterizer = Clusterizer(args.numPartitions)
    clusterizer.run()
