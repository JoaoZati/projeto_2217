import time


def filter_chunksize_to_csv(chunksize, path_export):
    for i, chunk in enumerate(chunksize):
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

        if i == 0:
            df_csv = chunk
            continue

        df_csv = df_csv.append(chunk)

    df_csv.to_csv(path_export)


def append_csvs_into_database(chucksize, engine):
    for i, df in enumerate(chucksize):
        print(f'Iniciando apprending linhas {(i+1) * 1_000_000}')
        start_time = time.time()

        df.drop(['Unnamed: 0'], inplace=True, axis=1)

        lst_columns = list(df.columns)
        lst_columns = [
            word.replace('ó', '').replace('.', '').replace(' - MWh (MED_C c,j)', '_mwh_(med_c_c_j)').replace(' ', '_').lower()
            for word in lst_columns
        ]

        df.columns = lst_columns

        print(f'sending chucksize lines {(i+1) * 1_000_000}')
        df['cnpj_da_carga'] = df['cnpj_da_carga'].str.replace(',', '.').astype(float).astype(int)
        df.to_sql(name='consumo_horario_2019', con=engine, if_exists='append', index=False)

        end_time = time.time()
        print(f'Finalizado appending de linhas {(i+1) * 1_000_000} com o tempo de {end_time - start_time}')
