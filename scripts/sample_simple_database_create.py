from scripts import env_config
import pandas as pd
from sqlalchemy import create_engine

# Connecting to PostgreSQL by providing a sqlachemy engine
str_engine = 'postgresql://' + \
    env_config('POSTGRESQL_USER') + ':' + \
    env_config('POSTGRESQL_PASSWORD') + '@' + \
    env_config('POSTGRESQL_HOST_IP') + ':' + \
    env_config('POSTGRESQL_PORT') + '/' + \
    env_config('POSTGRESQL_DATABASE')

engine = create_engine(str_engine, echo=False)

# Creating a simple pandas DataFrame with two columns
lst_hello = ['hello1', 'hello2']
lst_world = ['world1', 'world2']
df = pd.DataFrame(data={'hello': lst_hello, 'world': lst_world})

# df.to_sql(name='helloworld', con=engine, if_exists='replace', index=False)
df.to_sql(name='helloworld', con=engine, if_exists='append', index=False)

data = pd.read_sql('SELECT * FROM helloworld', engine)
