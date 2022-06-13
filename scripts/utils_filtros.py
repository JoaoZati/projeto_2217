import time
from datetime import timedelta


def replace_word(word):
    replaces = {
        'ã': 'a',
        'ó': 'o',
        ' - MWh (MED_C c,j)': '_mwh',
        ' ': '_',
        'Cod.': 'cd'
    }

    for key, value in replaces.items():
        word = word.replace(key, value)

    return word.lower()


def filter_chunksize_to_csv(chunksize, path_export):
    for i, chunk in enumerate(chunksize):
        # Fazer filtros
        index_bool = chunk[
           (chunk['Ramo de Atividade'] == 'COMÉRCIO') |
           (chunk['Ramo de Atividade'] == 'ACR') |
           (chunk['Ramo de Atividade'] == 'SERVIÇOS')
        ].index

        chunk.drop(index_bool, inplace=True)

        columns_drop = [
            'Cód. Perfil',
            'Sigla',
            'Consumo no Ambiente Livre da parcela de carga - MWh (RC_AL c,j)',
            'Consumo de energia ajustado da parcela cativa da carga parcialmente livre - MWh (RC_CAT c,j)',
            'Consumo de energia ajustado de uma parcela de carga - MWh (RC c,j)',
        ]

        chunk.drop(columns_drop, inplace=True, axis=1)

        # Otimizar nome do header para não ter erro no portgresql e concertando cnpj_da_carga
        lst_columns = list(chunk.columns)
        lst_columns = [replace_word(word) for word in lst_columns]

        chunk.columns = lst_columns

        try:
            chunk['cnpj_da_carga'] = chunk['cnpj_da_carga'].str.replace(',', '.').astype(float).astype(int)
        except AttributeError:
            chunk['cnpj_da_carga'] = chunk['cnpj_da_carga'].replace(',', '.').astype(float).astype(int)

        if i == 0:
            df_csv = chunk
            continue

        df_csv = df_csv.append(chunk)

    df_csv.to_csv(path_export, index=False)


def append_chunk_into_database(chucksize, engine, name_table):
    for i, df in enumerate(chucksize):
        print(f'Iniciando apprending linhas {(i+1) * 1_000_000}')
        start_time = time.time()

        df.drop(['Unnamed: 0'], inplace=True, axis=1)

        lst_columns = list(df.columns)
        lst_columns = [replace_word(word) for word in lst_columns]

        df.columns = lst_columns

        print(f'sending chucksize lines {(i+1) * 1_000_000}')
        try:
            df['cnpj_da_carga'] = df['cnpj_da_carga'].str.replace(',', '.').astype(float).astype(int)
        except AttributeError:
            df['cnpj_da_carga'] = df['cnpj_da_carga'].replace(',', '.').astype(float).astype(int)

        df.to_sql(name=name_table, con=engine, if_exists='append', index=False)

        end_time = time.time()
        print(f'Finalizado appending de linhas {(i+1) * 1_000_000} com o tempo de {end_time - start_time}')


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
