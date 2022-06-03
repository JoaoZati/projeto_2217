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

conn = engine.connect().execution_options(
        stream_results=True)

chunksize = pd.read_sql('SELECT * FROM consumo_horario_2019', conn, chunksize=1_000_000)

for chunk in chunksize:
    print(chunk.head())
