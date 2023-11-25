import os
import vertica_python
from logging import Logger
from pathlib import Path
from airflow.models.variable import Variable


class VerticaConn:
    def __init__(self, log: Logger) -> None:
        self.log = log

        # Забираем параметры подлючения к бд из переменных Airflow, в таком виде только для тестирования
        host =     Variable.get("VERTIKA_HOST")
        port =     Variable.get("VERTIKA_PORT")
        user =     Variable.get("VERTIKA_USER")
        password = Variable.get("VERTIKA_PASSWORD")
        database = Variable.get("VERTIKA_DB")


        self.conn_info = {'host': host, 
                    'port': port,
                    'user': user,       
                    'password': password,
                    'database': database,
                    'autocommit': True
                }
    
    # Этот метод выполняет все скрипты, перечисленные в дирректории с файлами SQL
    def sqls_executor(self, path_to_scripts: str, *args) -> None:

        files = os.listdir(path_to_scripts)
        file_paths = [Path(path_to_scripts, f) for f in files]
        file_paths.sort(key=lambda x: x.name)
        

        self.log.info(f"Found {len(file_paths)} files to apply changes.")
        i = 1
        print('Значение file_path следующее: ', file_paths)
        for fp in file_paths:
            print('Значение fp следующее: ', fp)
            self.log.info(f"Iteration {i}. Applying file {fp.name}")
            script = fp.read_text()
            print('Значение script следующее: ', script)
            with vertica_python.connect(**self.conn_info) as conn:
                cur = conn.cursor()
                cur.execute(script)

            self.log.info(f"Iteration {i}. File {fp.name} executed successfully.")
            i += 1

    # Этот метод выполняет один указанный скрипт
    def one_sql_executor(self, path_to_scripts: str, file_name) -> None:

        file_path = Path(path_to_scripts, file_name)
     
        print('Значение file_path следующее: ', file_path)

        self.log.info(f"Applying file {file_path}")
        script = file_path.read_text()
        with self._db as conn:
            cur = conn.cursor()
            cur.execute(script)

        self.log.info(f"File {file_path} executed successfully.")
 
