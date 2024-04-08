"""
Создайте в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location"
с полями id, name, type, dimension, resident_cnt.

С помощью API (https://rickandmortyapi.com/documentation/#location)
найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.

Запишите значения соответствующих полей этих трёх локаций в таблицу. resident_cnt — длина списка в поле residents.
"""

import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from g_maslov_plugins.g_maslov_ram_operator import GMaslovTop3LocationsByResidentsCount


DEFAULT_ARGS = {
    'owner': 'g-maslov',
    'start_date': days_ago(1)
}

with DAG("g-maslov-ram-task",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS
         ) as dag:

    extract_data = GMaslovTop3LocationsByResidentsCount(
        task_id='extract_data'
    )

    def prepare_target():
        # open connect
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

        sql = ('CREATE TABLE if not exists public.g_maslov_ram_location ( '
               'id int4 primary KEY, '
               '"name" varchar(255), '
               '"type" varchar(255), '
               'dimension varchar(255), '
               'residents_cnt int4 '
               ') '
               'DISTRIBUTED replicated;')

        pg_hook.run(sql, False)
        logging.info(f'executed sql:\n {sql}')

        sql = ('CREATE TABLE if not exists public.g_maslov_ram_location_stg ( '
               'id int4 primary KEY, '
               '"name" varchar(255), '
               '"type" varchar(255), '
               'dimension varchar(255), '
               'residents_cnt int4 '
               ') '
               'DISTRIBUTED replicated;')

        pg_hook.run(sql, False)
        logging.info(f'executed sql:\n {sql}')

        sql = 'TRUNCATE TABLE public.g_maslov_ram_location_stg;'
        pg_hook.run(sql, False)
        logging.info(f'executed sql:\n {sql}')

    prepare_target = PythonOperator(
        task_id='prepare_target',
        python_callable=prepare_target,
    )

    def insert_data_to_stg(**kwargs):
        # open connect
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

        # det data
        data = kwargs['ti'].xcom_pull(task_ids='extract_data')

        # insert data
        pg_hook.run(f'INSERT INTO public.g_maslov_ram_location_stg '
                    f'VALUES {data}')

    insert_data_to_stg = PythonOperator(
        task_id='insert_data_to_stg',
        python_callable=insert_data_to_stg,
    )

    def transactional_update(**kwargs):

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

        sql = ('BEGIN TRANSACTION; '
               'DELETE FROM public.g_maslov_ram_location; '
               'INSERT INTO public.g_maslov_ram_location '
               'SELECT * FROM public.g_maslov_ram_location_stg; '
               'COMMIT; ')

        pg_hook.run(sql, False)
        logging.info(f'executed sql:\n {sql}')

    transactional_update = PythonOperator(
        task_id='transactional_update',
        python_callable=transactional_update,
    )

    extract_data >> prepare_target >> insert_data_to_stg >> transactional_update
