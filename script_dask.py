from decouple import config
from dask import dataframe as dd

# no arquivo .env insira o camilho para baixar a planilha
DIR_EXCEL = config("DIR_EXCEL")

path_excel_2022_02 = DIR_EXCEL + 'Consumo_horario_2022_02.csv'

dd_2022_02 = dd.read_csv(path_excel_2022_02, sep=';')

print(len(dd_2022_02))