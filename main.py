import logging

from core.extractor import Extractor
from core.loader import Loader
from core.transformer import Transformer
from helpers.spark_helper import SparkHelper

logger = logging.getLogger(__name__)
INPUT_DIR = 'input/*.json'
TASK_ONE_OUTPUT_DIR = 'output/task_1/'
TASK_TWO_OUTPUT_DIR = 'output/task_2/'
METADATA_DIR = 'resources/metadata.json'
METADATA_KEY = 'input_metadata'
if __name__ == '__main__':
    """
    Entry point to the spark application
    """
    try:
        logger.info("Recipe aggregator app started")
        federated_data_frame = Extractor().extract(SparkHelper.get_spark_session(), 'json', INPUT_DIR,
                                                   METADATA_KEY,
                                                   METADATA_DIR)

        Loader().load(federated_data_frame, 'parquet', TASK_ONE_OUTPUT_DIR)
        curated_data_frame = Transformer().transform(SparkHelper.get_spark_session(), 'parquet', TASK_ONE_OUTPUT_DIR,
                                                     METADATA_KEY, METADATA_DIR)
        Loader().load(curated_data_frame, 'csv', TASK_TWO_OUTPUT_DIR)
        logger.info("Recipe aggregator app ended")
    except Exception as ex:
        logger.error("Failed to run ETL job .", ex)
