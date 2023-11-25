import logging

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable
from lib import VerticaConn


log = logging.getLogger(__name__)



@dag(
    schedule_interval=None,  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2023, 9, 1, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint6', 'init', 'drop'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint6_init_drop_table_dag():
    # Забираем параметры из переменных Airflow в таком виде только для тестирования
    ddl_path = Variable.get("INIT_DROP_DDL_FILES_PATH")


    # Объявляем таск, который создает структуру таблиц (выполнение скриптов)
    @task(task_id="drop_table")
    def drop_table():
        rest_loader = VerticaConn(log) # передаем парам для подключ и логир, получаем объкт для подлюч
        rest_loader.sqls_executor(ddl_path) # выполняем создание таблиц
        print(ddl_path)


    delete_s3_files = BashOperator(
        task_id='delete_s3_files',
        bash_command="rm -rf /data/*;",
        params={}
    )


    # Инициализируем объявленные таски.
    drop_table_ = drop_table()
    

    # Задаем последовательность выполнения тасков. У нас только инициализация схемы.
    drop_table_  >> delete_s3_files


# Вызываем функцию, описывающую даг.
init_drop_table_dag = sprint6_init_drop_table_dag()  # noqa
