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
