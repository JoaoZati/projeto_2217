from decouple import config
import pandas as pd

# no arquivo .env insira o camilho para baixar a planilha
DIR_EXCEL = config("DIR_EXCEL")

path_excel_2022_02 = DIR_EXCEL + 'Consumo_horario_2022_02.csv'

ch_2022_02 = pd.read_csv(path_excel_2022_02, sep=';', chunksize=10000)

list_capacidade_da_carga = []
i = 0
for chunk in ch_2022_02:
    i += 1
    coluna_cap = chunk['Capacidade da Carga (MW)']

    coluna_cap = coluna_cap.str.replace(',', '.')
    coluna_cap = coluna_cap.astype(float)

    media_capacidade_da_carga = coluna_cap.mean()
    list_capacidade_da_carga.append(media_capacidade_da_carga)
    print(media_capacidade_da_carga)
    print(i)

media_capacidade_final = sum(list_capacidade_da_carga)/len(list_capacidade_da_carga)
print(i)
