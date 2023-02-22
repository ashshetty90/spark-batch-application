import unittest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, DateType

import os
from helpers.common import get_schema
from helpers.spark_helper import SparkHelper

ROOT_DIR = os.path.split(os.path.abspath(os.path.dirname(__file__)))[0]
TEST_FILE_DIR = os.path.join(ROOT_DIR, 'tests/test_files/')
TEST_RECIPES_JSON_FILE = os.path.join(TEST_FILE_DIR, 'test_recipes.json')
TEST_CORRUPT_DATA_FILE = os.path.join(TEST_FILE_DIR, 'test_corrupt_data.json')
TEST_METADATA_JSON_FILE = os.path.join(TEST_FILE_DIR, 'test_metadata.json')


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

    def test_get_dataframe_reader(self):
        raw_df = SparkHelper.get_dataframe_reader(self.spark, 'json',
                                                  TEST_RECIPES_JSON_FILE,
                                                  get_schema('input_metadata', TEST_METADATA_JSON_FILE)
                                                  )
        self.assertIsInstance(raw_df, DataFrame)
        self.assertIsInstance(raw_df.schema["name"].dataType, StringType)
        self.assertIsInstance(raw_df.schema["datePublished"].dataType, DateType)

    def test_get_dataframe_reader_throws_exception(self):
        with self.assertRaises(Exception):
            SparkHelper.get_dataframe_reader(self.spark, 'json',
                                             TEST_CORRUPT_DATA_FILE,
                                             get_schema('input_metadata', TEST_METADATA_JSON_FILE)
                                             ).show()


if __name__ == '__main__':
    unittest.main()
