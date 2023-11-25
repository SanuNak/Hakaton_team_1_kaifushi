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
    tags=['sprint6', 'dds', 'load'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint6_uploading_to_dds_dag():
    # Забираем параметры из переменных Airflow в таком виде только для тестирования
    dml_path = Variable.get("DDS_LOAD_DML_FILES_PATH")

    # Объявляем таск, который создает структуру таблиц (выполнение скриптов)
    @task(task_id="uploading_to_dds")
    def uploading_to_dds():
        rest_loader = VerticaConn(log) # передаем парам для подключ и логир, получаем объкт для подлюч
        rest_loader.sqls_executor(dml_path) # выполняем создание таблиц
        print(f'Скрипты из каталога {dml_path} выполнены успешно!')

    # Инициализируем объявленные таски.
    uploading_to_dds_ = uploading_to_dds()

    # Задаем последовательность выполнения тасков. У нас только инициализация схемы.
    uploading_to_dds_


# Вызываем функцию, описывающую даг.
uploading_to_dds_dag = sprint6_uploading_to_dds_dag()  # noqa
