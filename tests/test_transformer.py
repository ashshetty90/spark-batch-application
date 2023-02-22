import unittest

from pyspark.sql.functions import col
from pyspark.sql.types import Row

from core.transformer import Transformer
from helpers.common import get_schema
from helpers.spark_helper import SparkHelper
import os

ROOT_DIR = os.path.split(os.path.abspath(os.path.dirname(__file__)))[0]
TEST_FILE_DIR = os.path.join(ROOT_DIR, 'tests/test_files/')
TEST_RECIPES_JSON_FILE = os.path.join(TEST_FILE_DIR, 'test_recipes.json')
TEST_METADATA_JSON_FILE = os.path.join(TEST_FILE_DIR, 'test_metadata.json')

class TransformerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.spark_session = SparkHelper.get_spark_session()
        self.raw_df = SparkHelper.get_dataframe_reader(self.spark_session, 'json', TEST_RECIPES_JSON_FILE,
                                                       get_schema('input_metadata', TEST_METADATA_JSON_FILE))

    def test_get_ingredients_with_beef(self):
        beef_df = Transformer().get_ingredients_with_beef(self.raw_df)
        self.assertEqual(beef_df.count(), 8)  # add assertion here

    def test_get_total_cook_time_dataframe(self):
        transformed_df = Transformer().get_total_cook_time_dataframe(self.spark_session, self.raw_df)
        total_cooking_time_list = [float(row['totalCookTime']) for row in
                                   transformed_df.select(col("totalCookTime")).filter(
                                       col("name").like("%Creamy%")).collect()]
        self.assertListEqual(total_cooking_time_list, [50.0, 140.0])

    def test_map_cook_time_to_difficulty(self):
        transformed_df = Transformer().get_total_cook_time_dataframe(self.spark_session, self.raw_df)
        cook_time_to_diff_df = Transformer().map_cook_time_to_difficulty(transformed_df)
        difficulty_list = [row['difficulty'] for row in
                           cook_time_to_diff_df.select(col("totalCookTime"), col("difficulty")).filter(
                               col("name").like("%Creamy%")).collect()]
        self.assertListEqual(difficulty_list, ['medium', 'hard'])

    def test_get_total_cooking_time(self):
        total_cook_time = Transformer().get_total_cooking_time("PT10M", "PT30M")
        self.assertEqual(total_cook_time, 40.0)

    def test_get_avg_total_cooking_time(self):
        transformed_df = Transformer().get_total_cook_time_dataframe(self.spark_session, self.raw_df)
        cook_time_to_diff_df = Transformer().map_cook_time_to_difficulty(transformed_df)
        actual_df = Transformer().get_avg_total_cooking_time(cook_time_to_diff_df).collect()
        expected_df = [
            Row(difficulty="medium", avg_total_cooking_time=47.31),

            Row(difficulty="hard", avg_total_cooking_time=135.0),

            Row(difficulty="easy", avg_total_cooking_time=22.0)
        ]
        self.assertEqual(expected_df, actual_df)


if __name__ == '__main__':
    unittest.main()
