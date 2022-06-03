from scripts import env_config
import pandas as pd
from sqlalchemy import create_engine
import time

from utils_filtros import append_csvs_into_database

start_program_time = time.time()
# Connecting to PostgreSQL by providing a sqlachemy engine
str_engine = 'postgresql://' + \
    env_config('POSTGRESQL_USER') + ':' + \
    env_config('POSTGRESQL_PASSWORD') + '@' + \
    env_config('POSTGRESQL_HOST_IP') + ':' + \
    env_config('POSTGRESQL_PORT') + '/' + \
    env_config('POSTGRESQL_DATABASE')

engine = create_engine(str_engine, echo=False)

# Setting spreaadsheet to append in database
DIR_EXCEL_FILTRADO = env_config("DIR_EXCEL_FILTRADO")

planilhas_filtradas = [
    # 'FIltrado_Consumo_horario_2019_1.csv',
    # 'Filtrado_Consumo_horario_2019_2.csv',
    # 'Filtrado_Consumo_horario_2019_3.csv',
    # 'Filtrado_Consumo_horario_2019_4.csv',
    # 'Filtrado_Consumo_horario_2019_5.csv',
    # 'Filtrado_Consumo_horario_2019_6.csv',
    # 'Filtrado_Consumo_horario_2019_7.csv',
    # 'Filtrado_Consumo_horario_2019_8.csv',
    # 'Filtrado_Consumo_horario_2019_9.csv',
    # 'Filtrado_Consumo_horario_2019_10.csv',
    # 'Filtrado_Consumo_horario_2019_11.csv',
    # 'Filtrado_Consumo_horario_2019_12.csv',
]

paths_excel = [DIR_EXCEL_FILTRADO + name_excel for name_excel in planilhas_filtradas]

for path_excel in paths_excel:
    print('#'*128)
    print(f'Planilha sendo salva no banco de dados  {path_excel}')
    start_time = time.time()

    # Chucksize and appending
    chucksize = pd.read_csv(path_excel, sep=',', chunksize=1_000_000)

    append_csvs_into_database(chucksize, engine)

    print(f'Finalizado a planilha {path_excel}')
    print(f'Total de tempo utilizado de {time.time() - start_time}')
    print('#' * 128)

end_program_time = time.time()
print(f'O Codigo foi finalizado com um total de tempo de {end_program_time - start_program_time}')
