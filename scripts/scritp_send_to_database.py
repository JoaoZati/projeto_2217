from scripts import env_config
import pandas as pd
from sqlalchemy import create_engine
from utils_filtros import append_csvs_into_database

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
path_excel = DIR_EXCEL_FILTRADO + 'Filtrado_Consumo_horario_2019_1.csv'

# Chucksize and appending
chucksize = pd.read_csv(path_excel, sep=',', chunksize=1_000_000)

append_csvs_into_database(chucksize, engine)


