import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from lib import VerticaConn


log = logging.getLogger(__name__)



@dag(
    schedule_interval=None,  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2023, 9, 1, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint6', 'init', 'create'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint6_init_create_table_dag():
    # Забираем параметры из переменных Airflow в таком виде только для тестирования
    ddl_path = Variable.get("INIT_CREATE_DDL_FILES_PATH")


    # Объявляем таск, который создает структуру таблиц (выполнение скриптов)
    @task(task_id="schema_init")
    def schema_init():
        rest_loader = VerticaConn(log) # передаем парам для подключ и логир, получаем объкт для подлюч
        rest_loader.sqls_executor(ddl_path) # выполняем создание таблиц
        print(ddl_path)

    # Инициализируем объявленные таски.
    init_schema = schema_init()

    # Задаем последовательность выполнения тасков. У нас только инициализация схемы.
    init_schema  # type: ignore


# Вызываем функцию, описывающую даг.
init_schema_dag = sprint6_init_create_table_dag()  # noqa
