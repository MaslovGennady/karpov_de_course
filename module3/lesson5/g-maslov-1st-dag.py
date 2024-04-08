"""
task 1:
создать даг из нескольких тасков:
— DummyOperator
— BashOperator с выводом даты
— PythonOperator с выводом даты
task2:
Доработать даг, который вы создали на прошлом занятии
Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)
Выводить результат работы в любом виде: в логах либо в XCom'е
"""

from airflow import DAG
import logging
import datetime as dt

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'owner': 'g-maslov',
    'start_date': dt.datetime(2022, 3, 1),
    'end_date': dt.datetime(2022, 3, 14)
}

with DAG("g-maslov-1st-dag",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS
         ) as dag:

    # 1st task
    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def print_ds(**kwargs):
        logging.info(kwargs['ds'])

    print_ds = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds,
        dag=dag
    )

    # 2nd task
    def read_data_from_gp(**kwargs):
        # open connect
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # eval parameter (weekday of ds)
        weekday = dt.datetime.strptime(kwargs['ds'], '%Y-%m-%d').isoweekday()

        # execute and log result
        cursor.execute(f'SELECT heading '
                       f'  FROM articles '
                       f' WHERE id = {weekday}')
        query_res = cursor.fetchall()
        logging.info('----RESULT----')
        logging.info('\n'.join([str(row) for row in query_res]))

        # close connect
        cursor.close()
        conn.close()

    read_data_from_greenplum = PythonOperator(
        task_id='read_data_from_greenplum',
        python_callable=read_data_from_gp,
    )

    dummy >> [echo_ds, print_ds] >> read_data_from_greenplum
