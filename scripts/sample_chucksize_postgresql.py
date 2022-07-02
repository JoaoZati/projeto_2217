from scripts import env_config
import pandas as pd
from sqlalchemy import create_engine

str_engine = 'postgresql://' + \
             env_config('POSTGRESQL_USER') + ':' + \
             env_config('POSTGRESQL_PASSWORD') + '@' + \
             env_config('POSTGRESQL_HOST_IP') + ':' + \
             env_config('POSTGRESQL_PORT') + '/' + \
             env_config('POSTGRESQL_DATABASE')

engine = create_engine(str_engine, echo=False)

conn = engine.connect().execution_options(
    stream_results=True)

chunksize = pd.read_sql('SELECT * FROM consumo_horario_2019_sample ORDER BY data', conn, chunksize=1_000_000)

for i, chunk in enumerate(chunksize):
    print(i*1_000_000, chunk.head())
