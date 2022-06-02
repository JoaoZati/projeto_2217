from scripts import env_config
import pandas as pd
import time

# no arquivo .env insira o camilho para baixar a planilha

DIR_EXCEL = env_config("DIR_EXCEL")

path_excel_2022_02 = DIR_EXCEL + 'Consumo_horario_2022_02.csv'

ch_2022_02 = pd.read_csv(path_excel_2022_02, sep=';', chunksize=300_000)

for chunk in ch_2022_02:
    chunk = chunk[chunk['Ramo de Atividade'] != 'COMÉRCIO']
    chunk = chunk[chunk['Ramo de Atividade'] != 'ACR']
    chunk = chunk[chunk['Ramo de Atividade'] != 'SERVIÇOS']

    chunk.to_csv(DIR_EXCEL + 'consumo_horario_100000_2022_02.csv')
    break
