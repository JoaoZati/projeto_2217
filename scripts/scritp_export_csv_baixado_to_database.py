from scripts import env_config
import pandas as pd
from sqlalchemy import create_engine
import time

from utils_filtros import append_chunk_into_database_baixado

# Connecting to PostgreSQL by providing a sqlachemy engine
str_engine = 'postgresql://' + \
    env_config('POSTGRESQL_USER') + ':' + \
    env_config('POSTGRESQL_PASSWORD') + '@' + \
    env_config('POSTGRESQL_HOST_IP') + ':' + \
    env_config('POSTGRESQL_PORT') + '/' + \
    env_config('POSTGRESQL_DATABASE')

engine = create_engine(str_engine, echo=False)

# Setting spreaadsheet to append in database
DIR_EXCEL = env_config("DIR_EXCEL")

# Nome da tabela que você irá fazer o append dos dados
nome_tabela_banco = 'consumo_horario_2019_sample'

# Planilhas que serão inseridas no banco de dados
planilhas_baixadas = [
    'Consumo_horario_2019_1.csv',
    # 'Consumo_horario_2019_2.csv',
    # 'Consumo_horario_2019_3.csv',
    # 'Consumo_horario_2019_4.csv',
    # 'Consumo_horario_2019_5.csv',
    # 'Consumo_horario_2019_6.csv',
    # 'Consumo_horario_2019_7.csv',
    # 'Consumo_horario_2019_8.csv',
    # 'Consumo_horario_2019_9.csv',
    # 'Consumo_horario_2019_10.csv',
    # 'Consumo_horario_2019_11.csv',
    # 'Consumo_horario_2019_12.csv',
]

start_program_time = time.time()

paths_excel = [DIR_EXCEL + name_excel for name_excel in planilhas_baixadas]

for path_excel in paths_excel:
    print('#'*128)
    print(f'Planilha sendo salva no banco de dados  {path_excel}')
    start_time = time.time()

    # Chucksize and appending
    chucksize = pd.read_csv(path_excel, sep=';', chunksize=1_000_000)

    append_chunk_into_database_baixado(chucksize, engine, nome_tabela_banco)

    print(f'Finalizado a planilha {path_excel}')
    print(f'Total de tempo utilizado de {time.time() - start_time}')
    print('#' * 128)

end_program_time = time.time()
print(f'O Codigo foi finalizado com um total de tempo de {end_program_time - start_program_time}')
