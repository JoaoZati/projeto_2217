from scripts import env_config
import pandas as pd
import time

# no arquivo .env insira o camilho para baixar a planilha

DIR_EXCEL_FILTRADO = env_config("DIR_EXCEL_FILTRADO")
nao_otimizada = True

path_excel_2022_02 = DIR_EXCEL_FILTRADO + 'Filtrado_Consumo_horario_2019_1.csv'

ch_2022_02 = pd.read_csv(path_excel_2022_02, sep=',', chunksize=200_000)

for chunk in ch_2022_02:

    chunk.drop(['Unnamed: 0'], inplace=True, axis=1)

    if nao_otimizada:
        lst_columns = list(chunk.columns)
        lst_columns = [
            word.replace('ó', '').replace('.', '').replace(' - MWh (MED_C c,j)', '_mwh_(med_c_c_j)').replace(' ',
                                                                                                             '_').lower()
            for word in lst_columns
        ]

        chunk.columns = lst_columns

        try:
            chunk['cnpj_da_carga'] = chunk['cnpj_da_carga'].str.replace(',', '.').astype(float).astype(int)
        except AttributeError:
            chunk['cnpj_da_carga'] = chunk['cnpj_da_carga'].replace(',', '.').astype(float).astype(int)

    chunk.to_csv(DIR_EXCEL_FILTRADO + 'sample_consumo_horario_2019_1.csv', index=False)
    break
