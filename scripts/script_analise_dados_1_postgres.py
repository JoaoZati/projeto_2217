from scripts import env_config
import pandas as pd
from sqlalchemy import create_engine
from datetime import timedelta, date
import json
import time

from utils_filtros import date_range


def calcular_quantidade_industrial(df_day, day):
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

    df_hour = df_day[df_day['hh'] == 0]
    df_result_count = df_hour.groupby(['ramo_de_atividade']).count()['data']
    df_result_capacidade = df_hour.groupby(['ramo_de_atividade']).sum()['capacidade_da_carga_(mw)']

    df_result_consumo_horario = df_day.groupby(['ramo_de_atividade']).sum()[
        'consumo_de_energia_no_ponto_de_conexao_da_parcela_de_carga_mwh'
    ]

    df_result = pd.concat([df_result_count, df_result_capacidade, df_result_consumo_horario], axis=1)
    df_result.columns = ['quantidade', 'demanda_acumulada_(mw)', 'consumo_horario_acumulado_(mwh)']

    df_result['demanda_media'] = df_result['demanda_acumulada_(mw)'] / df_result['quantidade']

    # horas_mes = number_of_hours_month(day.year, day.month)
    # df_result['consumo_horario_medio'] = df_result['consumo_horario_acumulado_(mwh)'] / horas_mes
    df_result['consumo_horario_medio'] = df_result['consumo_horario_acumulado_(mwh)'] / (24 * df_result['quantidade'])

    df_result['fator_de_carga_medio'] = df_result['consumo_horario_medio']/(df_result['demanda_media'])

    # df_result.to_csv('./2019_01_01_demanda.csv')

    return df_result.to_dict()


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

time_spend = time.time()
chunksize = pd.read_sql('SELECT * FROM consumo_horario_2019 ORDER BY data DESC', conn, chunksize=1_000_000)
print(f'Tempo para importar o chunksize: {time.time() - time_spend}s')
print('#'*128)

dct_results = {}
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

        if df_day.empty:
            dct_results[str_day] = {'empty': True}
            continue
        dct_results[str_day] = {'empty': False}

        dct_results[str_day]['dataframe'] = calcular_quantidade_industrial(df_day, day)
        print(f'Adicionado o dia {str_day} em {time.time() - time_day}s')

    print(f'Finalizado chunksize número: {i} linhas: {(i + 1) * 1_000_000} em: {time.time() - time_chunk}')
    print('-' * 128)

path_resultados = '/home/joao/Documents/mitsidi/2217/planilhas_analise/resultados/resultado_tabela.json'

open(path_resultados, 'w').write(json.dumps(dct_results))
