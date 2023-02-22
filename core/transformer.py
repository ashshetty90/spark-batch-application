from html import unescape
from isodate import parse_duration
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col, when, avg, round
from pyspark.sql.types import IntegerType, StringType, FloatType
import logging.config
from exceptions.exception import DataTransformationError
from helpers.common import get_schema
from helpers.spark_helper import SparkHelper

logger = logging.getLogger(__name__)


class Transformer:
    """
    Transformer class is responsible for holding the core transformation logic for the application.
    Given a dataframe it performs all kinds of cleaning and aggregation logic to create an aggregated
    Dataframe object which is then loaded into a persistent storage
    """

    def __init__(self) -> None:
        pass

    def get_total_cooking_time_udf(self):
        get_total_cooking_time_udf = udf(
            lambda ck_time, pr_time: self.get_total_cooking_time(ck_time, pr_time),
            IntegerType())
        return self.spark_session.udf.register("get_total_cooking_time_udf",
                                               get_total_cooking_time_udf)

    def get_total_cooking_time(self, cooking_time, prepping_time):
        """
        Converting ISO-8601 time deltas to floats and getting total cooking time
        :param cooking_time: str
        :param prepping_time: str
        :return: float
        """
        c_time = "PT0S" if not (cooking_time and cooking_time.strip()) else cooking_time.strip()
        p_time = "PT0S" if not (prepping_time and prepping_time.strip()) else prepping_time.strip()
        total_duration_in_seconds = (parse_duration(c_time).total_seconds() + parse_duration(
            p_time).total_seconds())
        return total_duration_in_seconds / 60

    def get_ingredients_with_beef(self, raw_df: DataFrame) -> DataFrame:
        """
        Filtering datframes that have beef in the ingredients
        :param raw_df: Dataframe
        :return: Dataframe
        """
        logger.info("Fetching recipes with beef")
        return SparkHelper.filter_data_set(raw_df, filter_key="ingredients", filter_value="beef")

    def get_total_cook_time_dataframe(self, spark, input_df) -> DataFrame:
        """
        Using spark udfs, get total cooking time by adding prep and cook time
        :param spark:  SparkSession
        :param input_df:
        :return: Dataframe
        """
        logger.info("Getting total cooking time")
        ## Registering UDFs
        remove_special_chars_udf = udf(lambda st: unescape(st.strip()),
                                       StringType())
        spark.udf.register("remove_special_chars_udf", remove_special_chars_udf)
        escape_newline_udf = udf(lambda st: st.replace("\n", "|"),
                                 StringType())
        spark.udf.register("escape_newline_udf", escape_newline_udf)
        get_total_cooking_time_udf = udf(lambda ck_time, pr_time: self.get_total_cooking_time(ck_time, pr_time),
                                         FloatType())
        spark.udf.register("get_total_cooking_time_udf", get_total_cooking_time_udf)
        return input_df.withColumn('totalCookTime', get_total_cooking_time_udf(col('cooktime'), col('prepTime')))

    def map_cook_time_to_difficulty(self, total_cook_time_df: DataFrame) -> DataFrame:
        """
        Maps the difficulty to total cooking time based on conditions
        :param total_cook_time_df: Dataframe
        :return: Dataframe
        """
        logger.info("Mapping cooking time to difficulty")
        return total_cook_time_df.withColumn('difficulty', when(col('totalCookTime').__le__(30), 'easy')
                                             .when((col('totalCookTime').__ge__(30) & col('totalCookTime').__le__(60)),
                                                   'medium')
                                             .otherwise('hard'))

    def get_avg_total_cooking_time(self, cook_time_to_diff_df: DataFrame) -> DataFrame:
        """
        Returns the average total cooking time dataframe from an input dataframe
        :param cook_time_to_diff_df: Dataframe
        :return: Dataframe
        """
        logger.info("Calculating average total cooking time")
        return cook_time_to_diff_df.groupBy('difficulty').agg(
            round(avg(col('totalCookTime')), 2).alias("avg_total_cooking_time")
        )

    def transform(self, spark_session, file_type, input_file_path, metadata_key, metadata_path) -> DataFrame:
        """
        Main method in the Transformer class binding all the different data manipulations and aggregations
                    :param metadata_path: str
                    :param metadata_key: str
                    :param spark_session : SparkSession
                        Configured SparkSession object
                    :param file_type : str
                       String specifying  type of the input file For Ex: csv, json or parquet
                    :param input_file_path : str
                       Path from the repository root containing the input files
                    :return : DataFrame
                    Spark Dataframe object containing the raw data with the relevant configurations
                """
        logger.info("Transform task started for file type {} at the input location".format(file_type, input_file_path))
        try:

            raw_df = SparkHelper.get_dataframe_reader(spark_session, file_type, input_file_path,
                                                      get_schema(metadata_key,
                                                                 metadata_path))
            recipes_with_beef_df = self.get_ingredients_with_beef(raw_df)
            total_cook_time_df = self.get_total_cook_time_dataframe(spark_session, recipes_with_beef_df)
            cook_time_to_diff_df = self.map_cook_time_to_difficulty(total_cook_time_df)
        except Exception as ex:
            logger.error("Failed to transform data .", ex)
            raise DataTransformationError(str(ex))
        avg_cooking_time_df = self.get_avg_total_cooking_time(cook_time_to_diff_df)
        logger.info("Transform task ended")
        return avg_cooking_time_df
