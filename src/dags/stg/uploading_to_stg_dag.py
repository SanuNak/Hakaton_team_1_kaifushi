import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from lib import VerticaConn
from lib import fetch_s3_file


log = logging.getLogger(__name__)



@dag(
    schedule_interval=None,  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2023, 9, 1, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint6', 'stg', 'load'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint6_uploading_to_stg_dag():
    # Забираем параметры из переменных Airflow в таком виде только для тестирования
    dml_path = Variable.get("STG_LOAD_DML_FILES_PATH")
    bucket_files = Variable.get("S3_FILES_LIST")
    bucket_files = bucket_files.split(",")
    stndrt_path = '/data/'

    @task(task_id="files_load")
    def files_load():
        for file in bucket_files:
            fetch_s3_file(bucket='sprint6', file_name=file, file_path=stndrt_path)

    # выведет первые десять строк каждого файла
    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command="""for file in {{ params.files }}; do head -n 10 "$file"; done;""",
        params={'files': " ".join([f'{stndrt_path}{f}' for f in bucket_files])}
    )

    # Объявляем таск, который создает структуру таблиц (выполнение скриптов)
    @task(task_id="uploading_to_stg")
    def uploading_to_stg():
        rest_loader = VerticaConn(log) # передаем парам для подключ и логир, получаем объкт для подлюч
        rest_loader.sqls_executor(dml_path) # выполняем создание таблиц
        print(dml_path)


    # Инициализируем объявленные таски.
    files_load_ = files_load()
    print_10_lines_of_each_ = print_10_lines_of_each
    uploading_to_stg_ = uploading_to_stg()

    # Задаем последовательность выполнения тасков. У нас только инициализация схемы.
    files_load_ >> print_10_lines_of_each_ >> uploading_to_stg_


# Вызываем функцию, описывающую даг.
uploading_to_stg_dag = sprint6_uploading_to_stg_dag()  # noqa
