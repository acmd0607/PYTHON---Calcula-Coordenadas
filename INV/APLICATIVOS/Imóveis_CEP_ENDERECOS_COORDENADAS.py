#!/usr/bin/env python3

import pandas as pd
import time
import os
import json
import requests
from geopy.geocoders import Nominatim 
from geopy.extra.rate_limiter import RateLimiter
# _________________________________________________________________________________________
#    ROTINA PARA IDENTIFICAR O NÚMERO DE LINHAS E COLUNAS DE UM DATAFRAME
# _________________________________________________________________________________________
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
# _________________________________________________________________________________________
#    ROTINA PARA VERIFICAR SE O SITE ESTÁ DISPONÍVEL
# _________________________________________________________________________________________
def verificar_disponibilidade_site(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return True
        else:
            return False
    except requests.exceptions.RequestException:
        return False
# _________________________________________________________________________________________
#   CONSULTA O CEP VIA A API DO BRASIL ABERTO (api_1)
# _________________________________________________________________________________________
def consulta_api_1(cep_formatado):

    cep = cep_formatado.replace('-', '')
    url = f'https://brasilaberto.com/api/v1/zipcode/{cep}'
    resposta = requests.get(url)
    logradouro = ''
    bairro = ''
    cidade = ''
    estado = ''
    status = 'ENDEREÇO NÃO ENCONTRADO'
    endereco_completo = logradouro + ', ' + bairro + ', ' + cidade + ', ' + estado
    if resposta.status_code == 200:
        conteudo = resposta.content.decode('utf-8')
        resposta.close()
        endereco = json.loads(conteudo)
        logradouro = endereco['result']['street']
        bairro = endereco['result']['district']
        cidade = endereco['result']['city']
        estado = endereco['result']['stateShortname']
        status = ''
        endereco_completo = logradouro + ', ' + bairro + ', ' + cidade + ', ' + estado 

    return logradouro, bairro, cidade, estado, status, endereco_completo
# _________________________________________________________________________________________
#   CONSULTA O CEP VIA A API DO VIA CEP (api_2)
# _________________________________________________________________________________________
def consulta_api_2(cep_formatado):
    
    cep = cep_formatado.replace('-', '')
    url = f'https://viacep.com.br/ws/{cep}/json/'
    headers = {'User-Agent': 'ACMD'}
    resposta = requests.request('GET', url, headers=headers)
    logradouro = ''
    bairro = ''
    cidade = ''
    estado = ''
    status = 'ENDEREÇO NÃO ENCONTRADO'
    endereco_completo = logradouro + ', ' + bairro + ', ' + cidade + ', ' + estado
    endereco_completo = ''
    if resposta.status_code == 200:
        conteudo = resposta.content.decode('utf-8')
        resposta.close()
        endereco = json.loads(conteudo)
        logradouro = endereco['logradouro']
        bairro = endereco['bairro']
        cidade = endereco['localidade']
        estado = endereco['uf']
        status = ''
        endereco_completo = logradouro + ', ' + bairro + ', ' + cidade + ', ' + estado 

    return logradouro, bairro, cidade, estado, status, endereco_completo
# _______________________________________________________________________________________________
#    ROTINA PARA PESQUISAR ENDEREÇOS A PARTIR DE CEP FORNECIDO, PODENDO UTILIZAR O API_1 OU API_2
# _______________________________________________________________________________________________
def encontra_enderecos(diretorio, endereco_arquivo_entrada, endereco_arquivo_saida, final_arquivo, planilha_entrada, planilha_saida, api):
    inicio = time.time()    
    print('\n      ---------------------------------------------------', 
          '\n      ROTINA PARA EFETUAR A PESQUISA DE ENDEREÇOS VIA CEP',
          '\n      ---------------------------------------------------')
    # ____________________________________________________________________________________________________________
    # FAZ A LEITURA DOS ARQUIVOS DE ENTRADA E SAIDA
    # ____________________________________________________________________________________________________________

    df_entrada = pd.read_excel(endereco_arquivo_entrada, sheet_name=planilha_entrada, index_col=None)
    numero_linhas_arquivo_entrada, numero_colunas_arquivo_entrada = informacoes(df_entrada)

    df_saida = pd.read_excel(endereco_arquivo_saida, sheet_name=planilha_saida, index_col=None)
    numero_linhas_arquivo_saida, numero_colunas_arquivo_saida = informacoes(df_saida)
    
    print('\n      -----> INICIO DA PESQUISA:')     
    print('      NÚMERO DE REGISTROS NO ARQUIVO ENTRADA:', numero_linhas_arquivo_entrada) 
    print('      NÚMERO DE REGISTROS NO ARQUIVO SAIDA:', numero_linhas_arquivo_saida)

    c_print = numero_linhas_arquivo_saida + 100
    c_gravacao = numero_linhas_arquivo_saida + 1000

    for i in range(numero_linhas_arquivo_saida, numero_linhas_arquivo_entrada, 1):

        if i == c_print:

            print('\n       Temos ', numero_linhas_arquivo_entrada, ' CEPs a processar, já concluimos ', i)
            c_print += 100

            final = time.time()
            dif = final - inicio
            dias, horas, minutos, segundos, dec, cent, mile = transforma_segundos(dif)
            p_print = '      Tempo parcial nesta etapa: {:2} dias, {:2} horas, {:2}, minutos, {:2} segundos, {:1} décimos, {:1} centésimos, {:5f} milésimos'.format(dias, horas, minutos, segundos, dec, cent, mile)
            print(p_print)

        imovel = df_entrada.iat[i, 0]
        cep = df_entrada.iat[i, 1]

        if api == 1:
            logradouro, bairro, cidade, estado, status, endereco_completo = consulta_api_1(cep)
        else:
            logradouro, bairro, cidade, estado, status, endereco_completo = consulta_api_2(cep)
        
        df_saida.loc[i] = [imovel, estado, cidade, bairro, logradouro, cep, status, endereco_completo]

        if i == c_gravacao:
            df_saida.to_excel(endereco_arquivo_saida, sheet_name=planilha_saida, header=True, index=False)
            c_gravacao += 1000

    df_saida.to_excel(endereco_arquivo_saida, sheet_name=planilha_saida, header=True, index=False)

    numero_linhas_arquivo_entrada, numero_colunas_arquivo_entrada = informacoes(df_entrada)
    numero_linhas_arquivo_saida, numero_colunas_arquivo_saida = informacoes(df_saida)

    print('\n      -----> FINAL DA PESQUISA:') 
    print('      NÚMERO DE REGISTROS NO ARQUIVO ENTRADA:', numero_linhas_arquivo_entrada) 
    print('      NÚMERO DE REGISTROS NO ARQUIVO SAIDA:', numero_linhas_arquivo_saida) 
    final = time.time()
    dif = final - inicio
    dias, horas, minutos, segundos, dec, cent, mile = transforma_segundos(dif)
    p_print = '\n      Tempo total nesta etapa: {:2} dias, {:2} horas, {:2}, minutos, {:2} segundos, {:1} décimos, {:1} centésimos, {:5f} milésimos'.format(dias, horas, minutos, segundos, dec, cent, mile)
    print(p_print)     

    return
# _________________________________________________________________________________________
#    ROTINA PARA PESQUISAR AS COORDENADAS GEOGRÁFICAS A PARTIR DE ENDEREÇO FORNECIDO
# _________________________________________________________________________________________
def encontra_coordenadas_geograficas(diretorio, endereco_arquivo_entrada, endereco_arquivo_saida, final_arquivo, planilha_entrada, planilha_saida):
    inicio = time.time()
    print('\n      ------------------------------------------------------------------------', 
          '\n      ROTINA PARA EFETUAR A PESQUISA DAS COORDENADAS GEOGRÁFICAS VIA ENDEREÇOS',
          '\n      ------------------------------------------------------------------------')

    df_entrada = pd.read_excel(endereco_arquivo_entrada, sheet_name=planilha_entrada, index_col=None)
    numero_linhas_arquivo_entrada, numero_colunas_arquivo_entrada = informacoes(df_entrada)
    
    df_saida = pd.read_excel(endereco_arquivo_saida, sheet_name=planilha_saida, index_col=None)
    numero_linhas_arquivo_saida, numero_colunas_arquivo_saida = informacoes(df_saida)
    
    print('\n      -----> INICIO DA PESQUISA:')     
    print('      NÚMERO DE REGISTROS NO ARQUIVO ENTRADA:', numero_linhas_arquivo_entrada) 
    print('      NÚMERO DE REGISTROS NO ARQUIVO SAIDA:', numero_linhas_arquivo_saida)

    geolocator = Nominatim(user_agent="my_app")                         
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)     
   
    c_print = numero_linhas_arquivo_saida + 100
    c_gravacao = numero_linhas_arquivo_saida + 1000

    for i in range(numero_linhas_arquivo_saida, numero_linhas_arquivo_entrada, 1):
        
        if i == c_print:

            c_print += 100
            print('\n      Temos ', numero_linhas_arquivo_entrada, ' ENDEREÇOS a processar, já foi concluido ', i)

            final = time.time()
            dif = final - inicio
            dias, horas, minutos, segundos, dec, cent, mile = transforma_segundos(dif)
            p_print = '      Tempo parcial nesta etapa: {:2} dias, {:2} horas, {:2}, minutos, {:2} segundos, {:1} décimos, {:1} centésimos, {:5f} milésimos'.format(dias, horas, minutos, segundos, dec, cent, mile)
            print(p_print)

        imovel = df_entrada.iat[i, 0]
        estado = df_entrada.iat[i, 1]
        cidade = df_entrada.iat[i, 2]
        bairro = df_entrada.iat[i, 3]
        logradouro = df_entrada.iat[i, 4]
        cep = df_entrada.iat[i, 5]
        status = df_entrada.iat[i, 6]
        endereco_completo = df_entrada.iat[i, 7]

        if status != 'ENDEREÇO NÃO ENCONTRADO':
            
            location = geocode(endereco_completo)
            
            if location is not None:

                latitude = round(location.latitude, 7)
                longitude = round(location.longitude, 7)

                coordenada = "POINT( " + str(latitude) + " " + str(longitude) + " )"
                df_saida.loc[i] = [imovel, estado, cidade, bairro, logradouro, cep, status, endereco_completo, coordenada]

            else:

                df_saida.loc[i] = [imovel, estado, cidade, bairro, logradouro, cep, status, endereco_completo, 'POINT EMPTY']
        else:

            df_saida.loc[i] = [imovel, estado, cidade, bairro, logradouro, cep, status, endereco_completo, ' ']

        if i == c_gravacao:
            df_saida.to_excel(endereco_arquivo_saida, sheet_name=planilha_saida, header=True, index=False)
            c_gravacao += 1000

    df_saida.to_excel(endereco_arquivo_saida, sheet_name=planilha_saida, header=True, index=False)
    
    numero_linhas_arquivo_entrada, numero_colunas_arquivo_entrada = informacoes(df_entrada)
    numero_linhas_arquivo_saida, numero_colunas_arquivo_saida = informacoes(df_saida)

    print('\n      -----> FINAL DA PESQUISA:') 
    print('      NÚMERO DE REGISTROS NO ARQUIVO ENTRADA:', numero_linhas_arquivo_entrada) 
    print('      NÚMERO DE REGISTROS NO ARQUIVO SAIDA:', numero_linhas_arquivo_saida)
    final = time.time()
    dif = final - inicio
    dias, horas, minutos, segundos, dec, cent, mile = transforma_segundos(dif)
    p_print = '\n      Tempo total nesta etapa: {:2} dias, {:2} horas, {:2}, minutos, {:2} segundos, {:1} décimos, {:1} centésimos, {:5f} milésimos'.format(dias, horas, minutos, segundos, dec, cent, mile)
    print(p_print)     
    return

if __name__ == '__main__':
    inicio = time.time()
    url1 = 'https://viacep.com.br'          # ESTE SITE CONTEM - API da viacep
    url2 = 'https://brasilaberto.com'       # ESTE SITE CONTEM - API da brasilaberto
    
    diretorio = 'C:/Users/acmdo/Meu Drive/PYTHON_Coordenadas/INV/'
    arquivo_origem = 'Imoveis_Aracaju'
    final_arquivo = '.xlsx'    
    api = 0
    # _________________________________________________________________________________________
    # VERIFICO QUAL API DEVO UTILIZAR 
    # _________________________________________________________________________________________
    if verificar_disponibilidade_site(url1) and api == 0:
        p_print = '      O site ' + url1 + ' está acessível.'
        print(p_print)
        p_print = '      Vou utilizar API deste site para identificar o endereçco a partir do CEP'
        print(p_print)
        api = 1
    elif verificar_disponibilidade_site(url2) and api == 0:
        p_print = '      O site ' + url2 + ' está acessível.'
        print(p_print)
        p_print = '      Vou utilizar API deste site para identificar o endereçco a partir do CEP'
        print(p_print)
        api = 2
    if api == 0:
        print('      ERRO: Não temos nenhum API disponível para identificar o endereçco a partir do CEP')
        print('            Volte a executar esse aplicativo mais tarde.')
    else:
        # _________________________________________________________________________________________
        # IDENTIFICO OS ARQUIVOS CRIADOS NO APLICATIVO DE "Cria_Arquivos"
        # _________________________________________________________________________________________
        arquivo_ceps = arquivo_origem + '_CEP'
        endereco_arquivo_ceps = diretorio + arquivo_ceps + final_arquivo
        planilha_ceps = 'ceps'
        arquivo_enderecos = arquivo_origem + '_ENDERECOS'
        endereco_arquivo_enderecos = diretorio + arquivo_enderecos + final_arquivo
        planilha_enderecos = 'cep_endereços'
        arquivo_coordenadas = arquivo_origem + '_ENDERECOS_COORDENADAS'
        endereco_arquivo_coordenadas = diretorio + arquivo_coordenadas + final_arquivo
        planilha_coordenadas = 'cep_endereços_coordenadas'
        # ____________________________________________________________________________________________________________
        #   SE OS ARQUIVOS CRIADOS NO APLICATIVO "Cria_Arquivos" NÃO EXISTIR, SUSPENDO O PROCESSAMENTO
        # ____________________________________________________________________________________________________________
        if os.path.isfile(endereco_arquivo_ceps) and os.path.isfile(endereco_arquivo_enderecos) and os.path.isfile(endereco_arquivo_coordenadas):
            # _________________________________________________________________________________________
            # ROTINA PARA PESQUISAR ENDEREÇO A PARTIR DO CEP
            # _________________________________________________________________________________________
            encontra_enderecos(diretorio, endereco_arquivo_ceps, endereco_arquivo_enderecos, final_arquivo, planilha_ceps, planilha_enderecos, api)
            # _________________________________________________________________________________________            
            # ROTINA PARA PESQUISAR COORDENADAS GEOGRÁFICAS A PARTIR DO ENDEREÇO
            # _________________________________________________________________________________________
            encontra_coordenadas_geograficas(diretorio, endereco_arquivo_enderecos, endereco_arquivo_coordenadas, final_arquivo, planilha_enderecos, planilha_coordenadas)
            final = time.time()
            dif = final - inicio
            dias, horas, minutos, segundos, dec, cent, mile = transforma_segundos(dif)
            p_print = '\n      Tempo total de processamento: {:2} dias, {:2} horas, {:2}, minutos, {:2} segundos, {:1} décimos, {:1} centésimos, {:5f} milésimos'.format(dias, horas, minutos, segundos, dec, cent, mile)
            print(p_print)
        else:
            p_print = '\n\n      ERRO: NÃO ENCONTREI O ARQUIVOS INICIAIS, EXECUTE O APLICATIVO DE "Cria_Arquivos".'
            print(p_print)