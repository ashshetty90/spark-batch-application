from pyspark.sql import DataFrame

import logging

from exceptions.exception import DataLoadError

logger = logging.getLogger(__name__)


class Loader:
    """
    Class to persist data into the specified file format and location
    """
    def load(self, final_df: DataFrame, file_type, output_path, mode="overwrite"):
        """
        Loading data into the files based on the file type, location and writing mode
        :param final_df: Dataframe
        :param file_type: str
        :param output_path: str
        :param mode: str
        :return:
        """
        logger.info(
            "Beginning loading data for file type {} at output location {} with mode {}".format(file_type, output_path,
                                                                                                mode))
        try:
            if file_type == 'csv':
                final_df.dropDuplicates().coalesce(1).write.mode(mode).csv(output_path, header=True)
            elif file_type == 'parquet':
                final_df.dropDuplicates().coalesce(1).write.mode(mode).parquet(output_path)
            else:
                final_df.dropDuplicates().coalesce(1).write.mode(mode).json(output_path)

        except Exception as ex:
            logger.error("Failed to load data .", ex)
            raise DataLoadError(str(ex))

        logger.info("Load job completed")
