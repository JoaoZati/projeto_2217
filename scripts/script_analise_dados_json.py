from scripts import env_config
import json
import pandas as pd

from utils_filtros import number_of_hours_month


def gerar_grande_dataframe(dct_rest):
    i = 0
    for key, value in dct_rest.items():
        df_day = pd.DataFrame.from_dict(value['dataframe'])

        df_day.reset_index(inplace=True)
        df_day.rename(columns={'index': 'ramo_de_atividade'}, inplace=True)

        df_day['data'] = key

        cols = df_day.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        df_day = df_day[cols]

        if i == 0:
            df_total = df_day.copy()
            i = 1
            continue
        df_total = pd.concat([df_total, df_day])

    df_total.reset_index(inplace=True)
    df_total.drop(columns=['index'], inplace=True)
    return df_total


def calcular_mes_de_dias(dct_days):
    numero_dias = 0
    for key, value in dct_days.items():
        numero_dias += 1
        df_day = pd.DataFrame.from_dict(value['dataframe'])

        if numero_dias == 1:
            df_month = df_day.copy()
            continue

        df_month['consumo_horario_acumulado_(mwh)'] = df_month['consumo_horario_acumulado_(mwh)'] + \
            df_day['consumo_horario_acumulado_(mwh)']

    horas_totais = number_of_hours_month(int(key.split('-')[0]), int(key.split('-')[1]))
    df_month['consumo_horario_medio'] = df_month['consumo_horario_acumulado_(mwh)'] / (
        horas_totais * df_month['quantidade']
    )

    df_month['fator_de_carga_medio'] = df_month['consumo_horario_medio'] / df_month['demanda_media']

    return df_month


DIR_RESULTADOS = env_config('DIR_RESULTADOS')
path_json = DIR_RESULTADOS + 'resultado_tabela_2019.json'
path_resultado_days_csv = DIR_RESULTADOS + 'resultados_dias_2019.csv'
path_resultado_month_json = DIR_RESULTADOS + 'resultados_tabela_meses_2019.json'
path_resultado_month_csv = DIR_RESULTADOS + 'resultados_tabela_meses_2019.csv'

with open(path_json, 'r') as file_json:
    dct_results_days = json.loads(file_json.read())

# Criar planilha resultado final days
df_total_days = gerar_grande_dataframe(dct_results_days)
df_total_days.to_csv(path_resultado_days_csv, index=False)

# CRIANDO DCT MONTHS
dct_months = {}
for i in range(1, 13):
    str_i = str(i) if i >= 10 else f'0{i}'
    dct_months[f'2019-{str_i}'] = {
        'dataframe': '',
        'days': {}
    }

# Fazendo append dos dias no dct month
for key, value in dct_results_days.items():
    key_month = key[:7]

    dct_months[key_month]['days'][key] = value

# Gerar json de meses
for key, value in dct_months.items():
    dct_months_days = value['days']
    df_month = calcular_mes_de_dias(dct_months_days)
    dct_months[key]['dataframe'] = pd.DataFrame.to_dict(df_month)

open(path_resultado_month_json, 'w').write(json.dumps(dct_months))

with open(path_resultado_month_json, 'r') as file_json:
    dct_results_months = json.loads(file_json.read())

df_total_months = gerar_grande_dataframe(dct_results_months)
df_total_months.to_csv(path_resultado_month_csv, index=False)
