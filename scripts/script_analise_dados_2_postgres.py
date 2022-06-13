from scripts import env_config
import pandas as pd
from sqlalchemy import create_engine
from datetime import date
import json
import time

from utils_filtros import date_range

path_spreedsheet = env_config("DIR_EXCEL_FILTRADO") + 'Filtrado_Consumo_horario_2019_1.csv'

str_engine = 'postgresql://' + \
             env_config('POSTGRESQL_USER') + ':' + \
             env_config('POSTGRESQL_PASSWORD') + '@' + \
             env_config('POSTGRESQL_HOST_IP') + ':' + \
             env_config('POSTGRESQL_PORT') + '/' + \
             env_config('POSTGRESQL_DATABASE')

engine = create_engine(str_engine, echo=False)

conn = engine.connect().execution_options(
    stream_results=True)


def calcular_codigo_de_carga(df_day):
    try:
        df_day.loc[:, 'capacidade_da_carga_(mw)'] = df_day.loc[:, 'capacidade_da_carga_(mw)'].str.replace(',', '.').astype(
            float
        )
        df_day.loc[:, 'consumo_de_energia_no_ponto_de_conexao_da_parcela_de_carga_mwh'] = df_day.loc[
            :,
            'consumo_de_energia_no_ponto_de_conexao_da_parcela_de_carga_mwh'
        ].str.replace(',', '.').astype(float)
    except AttributeError:
        pass

    gb_carga = df_day.groupby(['cd_carga', 'ramo_de_atividade', 'nome_empresarial', 'carga', 'capacidade_da_carga_(mw)'])

    df_carga_count = gb_carga.count()
    df_carga_count.reset_index(inplace=True)

    df_result = df_carga_count.loc[
        :,
        ['cd_carga', 'ramo_de_atividade', 'nome_empresarial', 'carga', 'capacidade_da_carga_(mw)']
    ]

    df_mean = gb_carga.mean().reset_index()['consumo_de_energia_no_ponto_de_conexao_da_parcela_de_carga_mwh']

    df_result = pd.concat([df_result, df_mean], axis=1)
    df_result.rename(
        columns={'consumo_de_energia_no_ponto_de_conexao_da_parcela_de_carga_mwh': 'consumo_medio_horario_mwh'},
        inplace=True
    )

    df_min = gb_carga.min().reset_index()['consumo_de_energia_no_ponto_de_conexao_da_parcela_de_carga_mwh']

    df_result = pd.concat([df_result, df_min], axis=1)
    df_result.rename(
        columns={'consumo_de_energia_no_ponto_de_conexao_da_parcela_de_carga_mwh': 'consumo_minimo_horario_mwh'},
        inplace=True
    )

    df_max = gb_carga.max().reset_index()['consumo_de_energia_no_ponto_de_conexao_da_parcela_de_carga_mwh']

    df_result = pd.concat([df_result, df_max], axis=1)
    df_result.rename(
        columns={'consumo_de_energia_no_ponto_de_conexao_da_parcela_de_carga_mwh': 'consumo_maximo_horario_mwh'},
        inplace=True
    )

    df_result['fator_de_carga_horario'] = df_result['consumo_medio_horario_mwh'] / df_result['capacidade_da_carga_(mw)']
    df_result['var_minimo_medio'] = abs(df_result['consumo_minimo_horario_mwh'] - df_result['consumo_medio_horario_mwh']) / \
        df_result['consumo_medio_horario_mwh']
    df_result['var_maximo_medio'] = abs(df_result['consumo_maximo_horario_mwh'] - df_result['consumo_medio_horario_mwh']) / \
        df_result['consumo_medio_horario_mwh']

    df_med = gb_carga.median().reset_index()['consumo_de_energia_no_ponto_de_conexao_da_parcela_de_carga_mwh']

    df_result = pd.concat([df_result, df_med], axis=1)
    df_result.rename(
        columns={'consumo_de_energia_no_ponto_de_conexao_da_parcela_de_carga_mwh': 'mediana_consumo_horario_mwh'},
        inplace=True
    )

    df_result['var_maximo_mediana'] = abs(
        df_result['consumo_maximo_horario_mwh'] - df_result['mediana_consumo_horario_mwh']
    ) / df_result['mediana_consumo_horario_mwh']

    df_result['data'] = df_day['data'].iloc[0]

    cols = df_result.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df_result = df_result[cols]

    return df_result


def chunksize_postgresql(chunksize_sample=1_000_000):
    # Connecting to PostgreSQL by providing a sqlachemy engine
    time_spend = time.time()
    chunksize = pd.read_sql('SELECT * FROM consumo_horario_2019 ORDER BY data', conn, chunksize=chunksize_sample)
    print(f'Tempo para importar o chunksize: {time.time() - time_spend}s')
    print('#'*128)

    return chunksize


time_chunk = time.time()
# chunksize = pd.read_csv(path_excel, sep=',', chunksize=1_000_000)
chunksize = chunksize_postgresql()
print(f'O tempo total para fazer o select do consumo filtrado foi: {time.time() - time_chunk}')


dt_last_day = 0
for i, chunk in enumerate(chunksize):
    time_chunk = time.time()
    if i == 0:
        df_last_day = pd.DataFrame(columns=chunk.columns)

    lst_first_day = [int(n) for n in chunk['data'][0].split('-')]
    lst_last_day = [int(n) for n in chunk['data'][len(chunk['data']) - 1].split('-')]

    # min para caso ele termine exatamente no final do dia, ai ele acabaria pulando
    dt_first_day = date(lst_first_day[0], lst_first_day[1], lst_first_day[2])
    dt_first_day = min(dt_first_day, dt_last_day) if dt_last_day else dt_first_day

    dt_last_day = date(lst_last_day[0], lst_last_day[1], lst_last_day[2])

    for i_day, day in enumerate(date_range(dt_first_day, dt_last_day)):
        time_day = time.time()
        str_day = day.strftime('%Y-%m-%d')
        df_day = chunk[chunk['data'] == str_day]

        if i_day == 0:
            df_day = pd.concat([df_last_day, df_day])
        elif i_day == len(date_range(dt_first_day, dt_last_day)) - 1:
            df_last_day = df_day
            if not day.month == 12 and day.day == 31:
                continue
            # Ultimo dia do ano ele faz a analise

        df_results = calcular_codigo_de_carga(df_day)

        df_results.to_sql(name='analise_carga_2019', con=engine, if_exists='append', index=False)
        print(f'Adicionado o dia {str_day} em {time.time() - time_day}s')

    print(f'Finalizado chunksize número: {i} linhas: {(i + 1) * 1_000_000} em: {time.time() - time_chunk}')
    print('-' * 128)
