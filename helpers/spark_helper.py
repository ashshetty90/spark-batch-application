from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lower, col
import logging.config

from exceptions.exception import DataExtractionError

logger = logging.getLogger(__name__)


class SparkHelper:
    """
    Helper class to get the common attributes required to operate on a spark Dataframe
    """
    @staticmethod
    def get_spark_session(mater_url="local[*]", app_name="RecipesAggregator", log_level="ERROR"):
        """
        Helper to get a spark session object. If not values are passed the default values are
        used to create a SparkSession object
        :param mater_url: str
        Number of cores to run spark on
        :param app_name: str
        Name to be assigned to the app
        :param log_level: str
        Level of logging to be configured
        :return: SparkSession
        """
        spark = SparkSession.builder.master(mater_url).appName(app_name).getOrCreate()
        spark.sparkContext.setLogLevel(log_level)
        return spark

    @staticmethod
    def filter_data_set(raw_data_frame, filter_key, filter_value):
        """
        Generic method that returns a filtered data frame when
        they value matches for a particular key in the input dataframe

        :param raw_data_frame: Dataframe
        :param filter_key: str
        :param filter_value: str
        :return: Dataframe
        """
        return raw_data_frame.filter(lower(col(filter_key)).contains(filter_value))

    @staticmethod
    def get_dataframe_reader(spark: SparkSession, file_type, file_path, schema):
        """
        Helper class that returns a Dataframe object
        :param schema: StructType
         Object containing the expected schema definition
        :param spark: SparkSession
        Session object containing the session and context information
        :param file_type: str
        String specifying  type of the input file For Ex: csv, json or parquet
        :param file_path: str
        Path from the repository root containing the input files
        :return:  spark_df: Dataframe
        """
        try:
            spark_df = None
            # Ugly hack of using if else instead of dictionary since there
            # was unexpected behaviour as the dictionary was loading all three and
            # switch case is supported after python 3.10
            if file_type == 'csv':
                spark_df = spark.read.schema(schema).option('header', 'true').option("mode", "FAILFAST").csv(
                    file_path)
            elif file_type == 'json':
                spark_df = spark.read.schema(schema).option("mode", "FAILFAST").json(file_path)
            elif file_type == 'parquet':
                spark_df = spark.read.schema(schema).option("mode", "FAILFAST").parquet(file_path)

        except Exception as ex:
            logger.error("Failed to read data from files.", ex)
            raise DataExtractionError(str(ex))

        return spark_df
