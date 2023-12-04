#!/usr/bin/env python3

import pandas as pd
import time
import os
import math
# ________________________________________________________________________
#   ROTINA PARA IDENTIFICAR O NÚMERO DE LINHAS E COLUNAS DE UM DATAFRAME
# ________________________________________________________________________
def informacoes(df):
    numero_de_linhas = df.shape[0]
    numero_de_colunas = df.shape[1]
    return (numero_de_linhas, numero_de_colunas)
# _________________________________________________________________________________________
#   ROTINA PARA TRANSFORMAR SEGUNDOS EM DIAS, HORAS, MINUTOS, SEGUNDOS, DÉCIMOS, CENTÉSIMOS E MILÉSIMOS
# _________________________________________________________________________________________
def transforma_segundos(segundo):
    dias = segundo // 86400
    horas = (segundo % 86400) // 3600
    minutos = ((segundo % 86400) % 3600) // 60
    segundos = ((segundo % 86400) % 3600) % 60
    decimos = (segundos - int(segundos)) * 10
    centesimos = (decimos - int(decimos)) * 10
    milesimos = (centesimos - int(centesimos)) * 10
    return int(dias), int(horas), int(minutos), int(segundos), int(decimos), int(centesimos), milesimos
# _________________________________________
#   ROTINA PARA LISTAR NÚMERO DE ANOMALIAS
# __________________________________________
def lista_anomalias(endereco_arquivo_entrada, df_entrada, numero_registros_entrada, numero_colunas_entrada):   
    p_print1 = '\n      ARQUIVO:' + endereco_arquivo_entrada
    p_print2 = '\n      NÚMERO DE REGISTROS: ' + str(numero_registros_entrada) + ' NÚMERO DE COLUNAS: ' + str(numero_colunas_entrada)
    print(p_print1, p_print2)

    anomalias_cep = 0
    anomalias_coordenadas = 0

    for i in range(0, numero_registros_entrada, 1):
        
        if df_entrada.iat[i, 6] == 'ENDEREÇO NÃO ENCONTRADO':
            anomalias_cep += 1
        
        if df_entrada.iat[i, 8] == 'POINT EMPTY':
            anomalias_coordenadas += 1

    p_anomalias_cep = anomalias_cep/numero_registros_entrada
    p_print = '      O número de ENDEREÇOS não encontrados: {:-5} o que representa: {:2.2%}'.format(anomalias_cep, p_anomalias_cep)
    print(p_print)
    p_anomalias_coordenadas = anomalias_coordenadas/numero_registros_entrada
    p_print = '      O número de COORDENADAS não encontradas: {:-5} o que representa: {:2.2%}'.format(anomalias_coordenadas, p_anomalias_coordenadas)
    print(p_print)
    anomalias = anomalias_cep + anomalias_coordenadas
    return anomalias
# _________________________________________________________________________________________
#   ROTINA PARA IDENTIFICAR AS COORDENADAS GEOGRÁFICAS E A DISTÂNCIA ENTRE ELAS
# _________________________________________________________________________________________
def processo(df_origem, numero_registros_origem, df_destino, numero_registros_destino, df_saida, endereco_arquivo_saida, nome_planilha_saida):
    point = 0
    key = 0
    cont = 0

    for i in range(0, numero_registros_origem, 1):
        
        if df_origem.iat[i, 6] != 'ENDEREÇO NÃO ENCONTRADO' and df_origem.iat[i, 8] != 'POINT EMPTY':

            for j in range(0, numero_registros_destino, 1):
                
                if df_destino.iat[j, 6] != 'ENDEREÇO NÃO ENCONTRADO' and df_destino.iat[j, 8] != 'POINT EMPTY':
                    
                    cont += 1
                    
                    coord_origem = df_origem.iat[i, 8]
                    locais = []
                    locais = [0 for x in range(0, 3, 1)]
                    x = 0
                    for k in range(0, len(coord_origem), 1):
                        if coord_origem[k] == " ":
                            locais[x] = k
                            x += 1
                    
                    lat_origem = float(coord_origem[locais[0]+1:locais[1]])
                    long_origem = float(coord_origem[locais[1]+1:locais[2]])

                    coord_destino = df_destino.iat[j, 8]
                    locais = []
                    locais = [0 for x in range(0, 3, 1)]
                    x = 0
                    for k in range(0, len(coord_destino), 1):
                        if coord_destino[k] == " ":
                            locais[x] = k
                            x += 1

                    lat_destino = float(coord_destino[locais[0]+1:locais[1]])
                    long_destino = float(coord_destino[locais[1]+1:locais[2]])

                    distancia = calcular_distancia(lat_origem, long_origem, lat_destino, long_destino)            
                    # ____________________________________________________________________________________________________________
                    #   IDENTIFICANDO E GUARDANDO TODAS AS DISTÂNCIAS MENORES OU IGUAIS A 5 km
                    # ____________________________________________________________________________________________________________
                    if distancia <= 5:

                        point += 1
                        key += 1

                        imovel = df_origem.iat[i, 0]
                        referencia = df_destino.iat[j, 0]
                        cep_origem = str(df_origem.iat[i, 5])
                        cep_destino = str(df_destino.iat[j, 5])

                        dist_1 = ''
                        dist_2 = ''
                        dist_3 = ''
                        dist_4 = ''
                        dist_5 = ''
                        if distancia <= 1:
                            dist_1 = distancia 
                        elif distancia > 1 and distancia <= 2:
                            dist_2 = distancia 
                        elif distancia > 2 and distancia <= 3:
                            dist_3 = distancia 
                        elif distancia > 3 and distancia <= 4:
                            dist_4 = distancia 
                        elif distancia > 4 and distancia <= 5:
                            dist_5 = distancia 
                        df_saida.loc[point] = [imovel, cep_origem, lat_origem, long_origem, referencia, cep_destino, lat_destino, long_destino, dist_1, dist_2, dist_3, dist_4, dist_5]
    # ____________________________________________________________________________________________________________
    #   FINALIZO INFORMANDO E SALVANDO NO ARQUIVO DE SAIDA AS DISTANCIAS ENCONTRADAS QUE FOREM MENORES QUE 5 KM
    # ____________________________________________________________________________________________________________
    p_print = '\n      Processei: {:-6} coordenadas, encontrei: {:-5} distância menores que 5 km.'.format(cont, point)
    df_saida.to_excel(endereco_arquivo_saida, sheet_name=nome_planilha_saida, header=True, index=False)
    print(p_print)
    return
# ____________________________________________________________________________________________________________
#   ROTINA PARA CALCULAR A DISTÂNCIA ENTRE DUAS COORDENADAS UTILIZANDO A FÓRMULA DE HAVERSINE 
# ____________________________________________________________________________________________________________
def calcular_distancia(lat1, lon1, lat2, lon2): 
    # ____________________________________________________________________________________________________________
    #   CONVERTE AS COORDENADAS DE GRAUS PARA RADIANOS
    # ____________________________________________________________________________________________________________
    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)
    # ____________________________________________________________________________________________________________
    #   SALVO O RAIO DA TERRA EM QUILÔMETROS
    # ____________________________________________________________________________________________________________
    raio_terra = 6371
    # ____________________________________________________________________________________________________________
    #   CALCULO AS DIFERENÇAS ENTRE AS LATITUDES E LONGITUDES DE ORIGEM E DESTINO
    # ____________________________________________________________________________________________________________
    dif_lat = lat2 - lat1
    dif_lon = lon2 - lon1
    # ____________________________________________________________________________________________________________
    #   UTILIZO A FÓRMULA DE HAVERSINE PARA O CÁLCULO DA DISTÂNCIA EM QUILÔMETROS
    # ____________________________________________________________________________________________________________
    a = math.sin(dif_lat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dif_lon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    distancia = raio_terra * c
    return distancia
    # ____________________________________________________________________________________________________________
    #   APLICATIVO PARA QUANTIFICAR O NÚMERO DE ANOMALIAS DE CEP E DE COORDENADAS NOS ARQUIVOS DE ORIGEM E DESTINO
    #   E CALCULAR AS DISTÂNCIAS ENTRE A ORIGEM E O DESTINO
    # ____________________________________________________________________________________________________________
if __name__ == '__main__':

    inicio = time.time()
    # ____________________________________________________________________________________________________________
    #   IDENTICANDO OS AQUIVOS A SEREM ACESSADOS
    # ____________________________________________________________________________________________________________
    diretorio = 'C:/Users/acmdo/Meu Drive/PYTHON_Coordenadas/INV/'
    final_arquivo = '.xlsx'
    nome_planilha = 'cep_endereços_coordenadas'

    arquivo_origem = 'Imoveis_Aracaju'
    nome_arquivo_origem = arquivo_origem + '_ENDERECOS_COORDENADAS'
    endereco_arquivo_origem = diretorio + nome_arquivo_origem + final_arquivo

    arquivo_destino = 'Referencias_Aracaju'
    nome_arquivo_destino = arquivo_destino + '_ENDERECOS_COORDENADAS'
    endereco_arquivo_destino = diretorio + nome_arquivo_destino + final_arquivo

    arquivo_saida = arquivo_origem + '_' + arquivo_destino + '_DISTANCIA'
    endereco_arquivo_saida = diretorio + arquivo_saida + final_arquivo
    nome_planilha_saida = 'cep_coordenadas_distancia'
    # ____________________________________________________________________________________________________________
    #   VERIFICO SE OS ARQUIVOS ORIGEM E DESTINO COM AS COORDENADAS EXISTEM
    # ____________________________________________________________________________________________________________
    if os.path.isfile(endereco_arquivo_origem) and os.path.isfile(endereco_arquivo_destino):
        # ____________________________________________________________________________________________________________
        #   QUANTIFICO O NÙMERO DE ANOMALIAS DE CEP E DE COORDENADAS NOS ARQUIVOS ORIGEM E DESTINO
        # ____________________________________________________________________________________________________________
        p_titulo1 = '\n      -----------------------------------------------------------------------------------------'
        p_titulo2 = '\n      QUANTIFICANDO O NÚMERO DE ANOMALIAS DE CEP E DE COORDENADAS NOS ARQUIVOS ORIGEM E DESTINO'
        print(p_titulo1, p_titulo2, p_titulo1)

        df_origem = pd.read_excel(endereco_arquivo_origem, sheet_name=nome_planilha, index_col=None)
        numero_registros_origem, numero_colunas_origem = informacoes(df_origem)
        anomalias_origem = lista_anomalias(endereco_arquivo_origem, df_origem, numero_registros_origem, numero_colunas_origem)

        df_destino = pd.read_excel(endereco_arquivo_destino, sheet_name=nome_planilha, index_col=None)
        numero_registros_destino, numero_colunas_destino = informacoes(df_destino)
        anomalias_destino = lista_anomalias(endereco_arquivo_destino, df_destino, numero_registros_destino, numero_colunas_destino)

        n_processos_origem = numero_registros_origem - anomalias_origem
        n_processos_destino = numero_registros_destino - anomalias_destino
        n_processos = n_processos_origem * n_processos_destino
        p_print = '\n      Vamos executar: {:-5} processos, considerando: {:-5} origens e {:-5} destinos'.format(n_processos, n_processos_origem, n_processos_destino)
        print(p_print)

        final = time.time()
        dif = final - inicio
        dias, horas, minutos, segundos, dec, cent, mile = transforma_segundos(dif)
        p_print = '\n      Tempo utilizado nesta etapa: {:2} dias, {:2} horas, {:2} minutos, {:2} segundos, {:1} décimos, {:1} centésimos, {:5f} milésimos'.format(dias, horas, minutos, segundos, dec, cent, mile)
        print(p_print)
        # ____________________________________________________________________________________________________________
        #   SE O ARQUIVO ONDE SERÃO SALVOS AS DISTÂNCIAS ENTRE AS COORDENADAS DE ORIGEM E DE DESTINO EXISTIR, EXCLUO
        # ____________________________________________________________________________________________________________
        if os.path.isfile(endereco_arquivo_saida):
            try:
                os.remove(endereco_arquivo_saida)
            except:
                p_print = '\n\n    **** ERRO no comando de remover o arquivo {:s} *** \n\n'.format(endereco_arquivo_saida)
                print(p_print)
        # ____________________________________________________________________________________________________________
        #   CRIA O ARQUIVO ONDE SERÃO SALVOS AS DISTÂNCIAS ENTRE AS COORDENADAS DE ORIGEM E DE DESTINO
        # ____________________________________________________________________________________________________________
        df_saida = pd.DataFrame(columns=['Imovel', 'cep_origem', 'latitude_origem', 'longitude_origem', 'Referencia', 'cep_destino', 'latitude_destino', 'longitude_destino',
                'distancia(km) <=1', 'distancia(km) <=2', 'distancia(km) <=3', 'distancia(km) <=4', 'distancia(km) <=5'])
        df_saida.to_excel(endereco_arquivo_saida, sheet_name=nome_planilha_saida, header=True, index=False)
        # ____________________________________________________________________________________________________________
        #   CALCULA AS DISTÂNCIAS ENTRE AS COORDENADAS DE ORIGEM E DE DESTINO UTILIZANDO A FÓRMULA DE HAVERSINE
        # ____________________________________________________________________________________________________________
        p_titulo1 = '\n      -----------------------------------------------------------------------------------------'
        p_titulo2 = '\n      CALCULANDO AS DISTÂNCIAS ENTRE ÀS COORDENADAS (origem) e (destino)'
        print(p_titulo1, p_titulo2, p_titulo1)
        processo(df_origem, numero_registros_origem, df_destino, numero_registros_destino, df_saida, endereco_arquivo_saida, nome_planilha_saida)
        final = time.time()
        dif = final - inicio
        dias, horas, minutos, segundos, dec, cent, mile = transforma_segundos(dif)
        p_print = '\n      Tempo total de processamento: {:2} dias, {:2} horas, {:2}, minutos, {:2} segundos, {:1} décimos, {:1} centésimos, {:5f} milésimos'.format(dias, horas, minutos, segundos, dec, cent, mile)
        print(p_print)
    else:
        p_print = '\n\n      ERRO: FALTA EXECUTAR A ROTINA QUE CALCULA OS ENDEREÇOS E COORDENADAS \n'
        print(p_print)