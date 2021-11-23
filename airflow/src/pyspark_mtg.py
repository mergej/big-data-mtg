import argparse
import sys
import re

import pyspark
import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, regexp_replace, udf
from pyspark.sql.types import *


def get_args():
    """
    Parses Command Line Arguments
    """
    parser = argparse.ArgumentParser(description='Some Basic Spark Job doing some stuff on Magic the Gathering data stored within HDFS.')
    parser.add_argument('--year', help='Partion Year To Process, e.g. 2019', required=True, type=str)
    parser.add_argument('--month', help='Partion Month To Process, e.g. 10', required=True, type=str)
    parser.add_argument('--day', help='Partion Day To Process, e.g. 31', required=True, type=str)
    parser.add_argument('--hdfs_source_dir', help='HDFS source directory, e.g. /user/hadoop/mtg', required=True, type=str)
    parser.add_argument('--hdfs_target_dir', help='HDFS target directory, e.g. /user/hadoop/mtg/final', required=True, type=str)
    parser.add_argument('--hdfs_target_format', help='HDFS target format, e.g. csv or parquet or...', required=True, type=str)

    return parser.parse_args()


if __name__ == '__main__':
    """
    Main Function
    """
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

    # Parse Command Line Args
    args = get_args()

    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    # specify hdfs raw file path
    input_path = args.hdfs_source_dir + args.year + '/' + args.month + '/' + args.day + '/*.csv'

    cards = spark.read.format('csv') \
        .option("header", "true") \
        .option("sep", ",") \
        .option("inferSchema", "true") \
        .option("quote", "\"") \
        .option("multiline", "true") \
        .option("encoding", "UTF-8") \
        .option("escape", "\"") \
        .load(input_path)
        
    final_columns = [
        "id",
        "artist",
        "availability", # array
        "borderColor",
        "colors", # array
        "convertedManaCost",
        "edhrecRank",
        "finishes", # array
        "flavorText",
        "keywords", # array
        "layout",
        "manaCost",
        "manaValue",
        "multiverseId",
        "name",
        "number",
        "power",
        "rarity",
        "setCode",
        "subtypes", # array
        "supertypes", # array
        "text",
        "toughness",
        "type",
        "types" # array
    ]

    array_columns = [
        "availability",
        "colors",
        "finishes",
        "keywords",
        "subtypes",
        "supertypes",
        "types"
    ]

    # drop unneeded columns
    for col in cards.columns:
        if col not in final_columns:
            cards = cards.drop(col)

    # remove linebreaks
    def change(value):
        return re.sub(r'\n', ' ', str(value))

    def modify_array(value):
        if isinstance(value, str):
            if value.startswith('"[\\"') and value.endswith('\\"]"'):
                return value.replace("\\", "")
        return value

    udf_change = F.udf(change, StringType())
    udf_array = F.udf(modify_array, StringType())

    cards = cards.withColumn("flavortext", udf_change("flavortext"))
    cards = cards.withColumn("text", udf_change("text"))

    for col in cards.columns:
        if col in array_columns:
            cards = cards.withColumn(col, udf_array(col))

    # write data to final hdfs path
    cards.coalesce(1) \
        .write \
        .format('csv') \
        .option("header", "true") \
        .option("sep","#") \
        .option("quote", "\"") \
        .option("encoding", "UTF-8") \
        .mode('overwrite') \
        .save('/user/hadoop/mtg/final/pyspark_mtg_cards.csv')
