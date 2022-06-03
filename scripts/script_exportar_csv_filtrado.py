from scripts import env_config
import pandas as pd
import time

from utils_filtros import filter_chunksize_to_csv

DIR_EXCEL = env_config("DIR_EXCEL")
DIR_EXCEL_FILTRADO = env_config("DIR_EXCEL_FILTRADO")

planilhas_filtrar = [
    'Consumo_horario_2019_1.csv',
    'Consumo_horario_2019_2.csv',
    'Consumo_horario_2019_3.csv',
    'Consumo_horario_2019_4.csv',
    'Consumo_horario_2019_5.csv',
    'Consumo_horario_2019_6.csv',
    'Consumo_horario_2019_7.csv',
    'Consumo_horario_2019_8.csv',
    'Consumo_horario_2019_9.csv',
    'Consumo_horario_2019_10.csv',
    'Consumo_horario_2019_11.csv',
    'Consumo_horario_2019_12.csv',
]

paths_excel = [DIR_EXCEL + name_excel for name_excel in planilhas_filtrar]

for path_excel, name_excel in zip(paths_excel, planilhas_filtrar):
    path_export = DIR_EXCEL_FILTRADO + 'Filtrado_' + name_excel
    chunksize = pd.read_csv(path_excel, sep=';', chunksize=1_000_000)

    print(f'Planilha Sendo Filtrada {name_excel}')
    start = time.time()

    filter_chunksize_to_csv(chunksize, path_export)

    total_time = time.time() - start
    print(f'Planilha Filtrada para {path_export}')
    print(f'Tempo total decorrido de {total_time}')
