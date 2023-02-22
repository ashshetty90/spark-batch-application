from pyspark.sql import DataFrame

from helpers.common import get_schema
from helpers.spark_helper import SparkHelper
import logging

logger = logging.getLogger(__name__)


class Extractor:
    """
    The extractor class is the first class in the ETL process. This class is responsible
    for extracting raw files from an input file location based on the format and outputting
    it to an output location based on the specified data format.
    """

    def __init__(self) -> None:
        pass

    def extract(self, spark, file_type, input_file_path, key, metadata_file_path) -> DataFrame:
        """
            :param spark : SparkSession
                Configured SparkSession object
            :param file_type : str
               String specifying  type of the input file For Ex: csv, json or parquet
            :param input_file_path : str
               Path from the repository root containing the input files
            :param key: str
                key from the metadata file mapped to the schema
            :param metadata_file_path: str
                Path from the repository root containing the metadata json file
            :return : DataFrame
            Spark Dataframe object containing the raw data with the relevant configurations
        """
        logger.info("Extract job started from {} directory".format(input_file_path))
        return SparkHelper.get_dataframe_reader(spark, file_type,
                                                input_file_path, get_schema(key, metadata_file_path))
