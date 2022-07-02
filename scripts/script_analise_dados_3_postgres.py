"""
Modulo para fazer o filtro da empresa do setor de aluminio
"""

from scripts import env_config
import pandas as pd
from sqlalchemy import create_engine
import time

time_start = time.time()

str_engine = 'postgresql://' + \
    env_config('POSTGRESQL_USER') + ':' + \
    env_config('POSTGRESQL_PASSWORD') + '@' + \
    env_config('POSTGRESQL_HOST_IP') + ':' + \
    env_config('POSTGRESQL_PORT') + '/' + \
    env_config('POSTGRESQL_DATABASE')

engine = create_engine(str_engine, echo=False)

conn = engine.connect().execution_options(stream_results=True)

chunksize = pd.read_sql('SELECT * FROM consumo_horario_2019 ORDER BY data', conn, chunksize=1_000_000)

for i, chunk in enumerate(chunksize):
    print(f'{i + 1}m linhas')
    time_i = time.time()

    df = chunk.loc[
        (chunk['nome_empresarial'] == 'ASA ALUMINIO S.A') |
        (chunk['nome_empresarial'] == 'COMPANHIA BRASILEIRA DE ALUMINIO') |
        (chunk['nome_empresarial'] == 'ALBRAS ALUMINIO BRASILEIRO S/A') |
        (chunk['nome_empresarial'] == 'EXAL BRASIL - FABRICACAO DE EMBALAGENS DE ALUMINIO LTDA.') |
        (chunk['nome_empresarial'] == 'ALCAN ALUMINA LTDA') |
        (chunk['nome_empresarial'] == 'TERA METAIS ALUMINIO LTDA.') |
        (chunk['nome_empresarial'] == 'ALUMICONTE COMPONENTES DE ALUMINIO EIRELI') |
        (chunk['nome_empresarial'] == 'ACPA ANODIZACAO DE CHAPAS E PERFIS DE ALUMINIO LTDA') |
        (chunk['nome_empresarial'] == 'PERFIL ALUMINIO DO BRASIL S/A') |
        (chunk['nome_empresarial'] == 'ALUNORTE ALUMINA DO NORTE DO BRASIL S/A') |
        (chunk['nome_empresarial'] == 'ALUMINIO ARARAS LTDA') |
        (chunk['nome_empresarial'] == 'ALCOA ALUMINIO S/A')
    ]

    lst_columns = [
        'nome_empresarial',
        'data',
        'hh',
        'cd_carga',
        'carga',
        'submercado',
        'capacidade_da_carga_(mw)',
        'consumo_de_energia_no_ponto_de_conexao_da_parcela_de_carga_mwh',
        'ramo_de_atividade'
    ]

    df = df.loc[:, lst_columns]

    df['subsegmento'] = 'Alumínio'

    if i == 0:
        df_final = df
    else:
        df_final = pd.concat([df_final, df])

    print(f'total time filter: {time.time() - time_i}')
    print('#'*126)

# Exportar para um csv e o banco de dados
nome_planilha = 'consumo_horario_2019_aluminio'

path_export_csv = env_config('DIR_RESULTADOS') + nome_planilha + '.csv'
df_final.to_csv(path_export_csv, index=False)

df_final.to_sql(nome_planilha, con=engine, if_exists='replace', index=False)

[print('#'*126) for _ in range(3)]
print(time.time() - time_start)
[print('#'*126) for _ in range(3)]
