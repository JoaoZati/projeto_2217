from scripts import env_config
import pandas as pd
from sqlalchemy import create_engine

from utils_filtros import export_chunk_into_csv

str_engine = 'postgresql://' + \
             env_config('POSTGRESQL_USER') + ':' + \
             env_config('POSTGRESQL_PASSWORD') + '@' + \
             env_config('POSTGRESQL_HOST_IP') + ':' + \
             env_config('POSTGRESQL_PORT') + '/' + \
             env_config('POSTGRESQL_DATABASE')

engine = create_engine(str_engine, echo=False)

conn = engine.connect().execution_options(
    stream_results=True)

chunksize = pd.read_sql('SELECT * FROM analise_carga_2019', conn, chunksize=1_000_000)

path_export_csv = env_config("DIR_RESULTADOS") + 'analise_carga_2019.csv'

export_chunk_into_csv(chunksize, path_export_csv)
