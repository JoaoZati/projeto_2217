from scripts import env_config
import pandas as pd
from sqlalchemy import create_engine
from datetime import timedelta, date
import json
import time


def date_range(date1, date2):
    """

    Parameters
    ----------
    date1: datetime.date: data inicial do range a ser analisado
    date2: datatime.date: data final do range a ser analisado

    Returns
    -------
    list: lista contendo o range de todas as datas (apenas dias) entre as duas datas fornecidas.
    """

    return [
        date1 + timedelta(n) for n in range(int((date2 - date1).days) + 1)
    ]


def number_of_hours_month(year: int, month: int) -> int:
    """

    Parameters
    ----------
    year: int, numero do ano da data selecionada
    month: int, numero do mes da data selecinada, ex: janeiro -> 1, fevereiro -> 2...

    Returns
    -------
    int, numero de horas naquele mes

    """
    leap_year = 0
    if (year % 4 == 0) and not (year % 100 == 0) or (year % 400 == 0):
        leap_year = 1

    lst_months = [31, 28 + leap_year, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    return 24 * lst_months[month - 1]


def calcular_quantidade_industrial(df_day, day):
    df_day.loc[:, 'capacidade_da_carga_(mw)'] = df_day.loc[:, 'capacidade_da_carga_(mw)'].str.replace(',', '.').astype(
        float
    )
    df_day.loc[:, 'consumo_de_energia_no_ponto_de_conexao_da_parcela_de_carga_mwh'] = df_day.loc[
        :,
        'consumo_de_energia_no_ponto_de_conexao_da_parcela_de_carga_mwh'
    ].str.replace(',', '.').astype(float)

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
chunksize = pd.read_sql('SELECT * FROM consumo_horario_2019 ORDER BY data', conn, chunksize=1_000_000)
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
            continue

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

with open(path_resultados, 'r') as file_json:
    dct_results_2 = json.loads(file_json.read())

df_open_results = pd.DataFrame.from_dict(dct_results_2['2019-01-02']['dataframe'])
