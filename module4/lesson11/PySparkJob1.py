import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def process(spark, flights_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param result_path: путь с результатами преобразований
    """
    flights = spark.read.parquet(flights_path)
    # flights.show(truncate=False, n=4)

    top_10_flights = flights \
        .where(flights['TAIL_NUMBER'].isNotNull()) \
        .groupBy(flights['TAIL_NUMBER']) \
        .agg(F.count(flights['TAIL_NUMBER']).alias('count')) \
        .select(F.col('TAIL_NUMBER'), F.col('count')) \
        .orderBy(F.col('count').desc()) \
        .limit(10)

    # top_10_flights.show(truncate=False)
    top_10_flights.write.format("parquet").mode("overwrite").save(result_path)

def main(flights_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob1').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    result_path = args.result_path
    main(flights_path, result_path)
