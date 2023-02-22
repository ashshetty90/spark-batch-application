import json
from pyspark.sql.types import *

MAPPING = {
    "string": StringType(),
    "integer": IntegerType(),
    "double": DoubleType(),
    "timestamp": TimestampType(),
    "date": DateType(),
    "long": LongType()
}


def get_schema(key, config_file_path) -> StructType:
    """
    Mapping schema file to the raw input data
    :param key: str
    :param config_file_path: str
    :return: StructType
    """
    with open(config_file_path) as config_file:
        data = json.load(config_file)
        table_dict = data[key]
        fields = [StructField(field, MAPPING.get(table_dict.get(field)), True) for field in table_dict]

    return StructType(fields)
