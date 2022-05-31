from decouple import config

# no arquivo .env insira o camilho para baixar a planilha
DIR_EXCEL = config("DIR_EXCEL")
print(DIR_EXCEL)
