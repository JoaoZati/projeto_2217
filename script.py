from decouple import config
import pandas as pd

# no arquivo .env insira o camilho para baixar a planilha
DIR_EXCEL = config("DIR_EXCEL")

path_excel_2022_02 = DIR_EXCEL + 'Consumo_horario_2022_02.csv'

ch_2022_02 = pd.read_csv(path_excel_2022_02, sep=';', chunksize=1000000)

for chunk in ch_2022_02:
    print(chunk)
