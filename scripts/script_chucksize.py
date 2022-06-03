from scripts import env_config
import pandas as pd
import time

# no arquivo .env insira o camilho para baixar a planilha
start_file = time.time()
DIR_EXCEL_FILTRADO = env_config("DIR_EXCEL_FILTRADO")

path_excel_2022_02 = DIR_EXCEL_FILTRADO + 'Filtrado_Consumo_horario_2019_1.csv'

ch_2022_02 = pd.read_csv(path_excel_2022_02, sep=',', chunksize=1_000_000)

list_capacidade_da_carga = []
i = 0
for chunk in ch_2022_02:
    chunk = chunk[chunk['Ramo de Atividade'] != 'COMÉRCIO']
    chunk = chunk[chunk['Ramo de Atividade'] != 'ACR']
    chunk = chunk[chunk['Ramo de Atividade'] != 'SERVIÇOS']

    db_a = chunk[chunk['Ramo de Atividade'] == 'ALIMENTÍCIOS']

    start = time.time()
    i += 1
    coluna_cap = db_a['Capacidade da Carga (MW)']

    coluna_cap = coluna_cap.str.replace(',', '.')
    coluna_cap = coluna_cap.astype(float)

    media_capacidade_da_carga = coluna_cap.mean()
    list_capacidade_da_carga.append(media_capacidade_da_carga)

    end = time.time()
    print(i)
    print(end - start)
    print(media_capacidade_da_carga)


media_capacidade_final = sum(list_capacidade_da_carga)/len(list_capacidade_da_carga)
end_file = time.time()

print(i)
print(media_capacidade_final)
print(end_file - start_file)
