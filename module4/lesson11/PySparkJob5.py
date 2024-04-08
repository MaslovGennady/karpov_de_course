import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def process(spark, flights_path, airlines_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param result_path: путь с результатами преобразований
    """
    flights = spark.read.parquet(flights_path)
    airlines = spark.read.parquet(airlines_path)
    # flights.show(truncate=False, n=4)

    flights_data = (
        flights
        .withColumnRenamed('AIRLINE', 'AIRLINE_IATA_CODE')
        .join(other=airlines.withColumnRenamed('AIRLINE', 'AIRLINE_NAME'),
              on=airlines['IATA_CODE'] == F.col('AIRLINE_IATA_CODE'), how='inner')
        ).groupBy(F.col('AIRLINE_NAME'))\
        .agg(
            F.sum(
                F.when((F.col('DIVERTED').eqNullSafe(0)) &
                       (F.col('CANCELLED').eqNullSafe(0)), 1).otherwise(0)).alias('correct_count'),
            # F.count(F.col('FLIGHT_NUMBER')).alias('correct_count'),
            F.sum(F.when(F.col('DIVERTED').eqNullSafe(1), 1).otherwise(0)).alias('diverted_count'),
            F.sum(F.when(F.col('CANCELLED').eqNullSafe(1), 1).otherwise(0)).alias('cancelled_count'),
            F.avg('DISTANCE').alias('avg_distance'),
            F.avg('AIR_TIME').alias('avg_air_time'),
            F.sum(F.when(F.col('CANCELLATION_REASON').eqNullSafe('A'), 1).otherwise(0)).alias('airline_issue_count'),
            F.sum(F.when(F.col('CANCELLATION_REASON').eqNullSafe('B'), 1).otherwise(0)).alias('weather_issue_count'),
            F.sum(F.when(F.col('CANCELLATION_REASON').eqNullSafe('C'), 1).otherwise(0)).alias('nas_issue_count'),
            F.sum(F.when(F.col('CANCELLATION_REASON').eqNullSafe('D'), 1).otherwise(0)).alias('security_issue_count')
            # F.sum(F.when( F.col('LATE_AIRCRAFT_DELAY') > 0, 1).otherwise(0)).alias('airline_issue_count'),
            # F.sum(F.when( F.col('WEATHER_DELAY') > 0, 1).otherwise(0)).alias('weather_issue_count'),
            # F.sum(F.when( F.col('AIR_SYSTEM_DELAY') > 0, 1).otherwise(0)).alias('nas_issue_count'),
            # F.sum(F.when( F.col('SECURITY_DELAY') > 0, 1).otherwise(0)).alias('security_issue_count')
        ) \
        .select(
            F.col('AIRLINE_NAME'),
            F.col('correct_count'),
            F.col('diverted_count'),
            F.col('cancelled_count'),
            F.col('avg_distance'),
            F.col('avg_air_time'),
            F.col('airline_issue_count'),
            F.col('weather_issue_count'),
            F.col('nas_issue_count'),
            F.col('security_issue_count')
        )

    # flights_data.show(truncate=False)
    flights_data.write.format("parquet").mode("overwrite").save(result_path)


def main(flights_path, airlines_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob5').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='airlines.parquet',
                        help='Please set airlines datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    result_path = args.result_path
    main(flights_path, airlines_path, result_path)
