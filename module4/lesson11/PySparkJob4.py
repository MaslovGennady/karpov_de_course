import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def process(spark, flights_path, airlines_path, airports_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param airports_path: путь до датасета c аэропортами
    :param result_path: путь с результатами преобразований
    """
    flights = spark.read.parquet(flights_path)
    airlines = spark.read.parquet(airlines_path)
    airports = spark.read.parquet(airports_path)
    # flights.show(truncate=False, n=4)

    flights = (
        flights.select(
                F.col('TAIL_NUMBER'),
                F.col('AIRLINE'),
                F.col('ORIGIN_AIRPORT'),
                F.col('DESTINATION_AIRPORT')
            )
        ).join(other=airlines.withColumnRenamed('AIRLINE', 'AIRLINE_NAME'),
               on=airlines['IATA_CODE'] == F.col('AIRLINE'), how='inner') \
         .join(other=airports.withColumnRenamed('COUNTRY', 'ORIGIN_COUNTRY')
                             .withColumnRenamed('AIRPORT', 'ORIGIN_AIRPORT_NAME')
                             .withColumnRenamed('LATITUDE', 'ORIGIN_LATITUDE')
                             .withColumnRenamed('LONGITUDE', 'ORIGIN_LONGITUDE')
                             .withColumnRenamed('IATA_CODE', 'ORIGIN_IATA_CODE'),
               on=F.col('ORIGIN_IATA_CODE') == F.col('ORIGIN_AIRPORT'), how='inner') \
         .join(other=airports.withColumnRenamed('COUNTRY', 'DESTINATION_COUNTRY')
                             .withColumnRenamed('AIRPORT', 'DESTINATION_AIRPORT_NAME')
                             .withColumnRenamed('LATITUDE', 'DESTINATION_LATITUDE')
                             .withColumnRenamed('LONGITUDE', 'DESTINATION_LONGITUDE')
                             .withColumnRenamed('IATA_CODE', 'DESTINATION_IATA_CODE'),
               on=F.col('DESTINATION_IATA_CODE') == F.col('DESTINATION_AIRPORT'), how='inner') \
         .select(
            F.col('AIRLINE_NAME'),
            F.col('TAIL_NUMBER'),
            F.col('ORIGIN_COUNTRY'),
            F.col('ORIGIN_AIRPORT_NAME'),
            F.col('ORIGIN_LATITUDE'),
            F.col('ORIGIN_LONGITUDE'),
            F.col('DESTINATION_COUNTRY'),
            F.col('DESTINATION_AIRPORT_NAME'),
            F.col('DESTINATION_LATITUDE'),
            F.col('DESTINATION_LONGITUDE')
         )

    # flights.show(truncate=False)
    flights.write.format("parquet").mode("overwrite").save(result_path)


def main(flights_path, airlines_path, airports_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, airports_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob4').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='airlines.parquet', help='Please set airlines datasets path.')
    parser.add_argument('--airports_path', type=str, default='airports.parquet', help='Please set airports datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    airports_path = args.airports_path
    result_path = args.result_path
    main(flights_path, airlines_path, airports_path, result_path)
