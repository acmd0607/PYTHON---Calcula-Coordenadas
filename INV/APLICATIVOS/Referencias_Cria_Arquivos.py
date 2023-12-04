#!/usr/bin/env python3
import pandas as pd
import time
import os
# ____________________________________________________________________________________________________________
# VERIFICA SE OS NÚMEROS QUE IDENTIFICAM OS CEP's ESTÃO CORRETOS
# SE TODOS OS CEP´s ESTIVEREM CORRETOS GERA OS ARQUIVOS (1) IDENTIFICAR ENDEREÇO E (2) IDENTIFICAR COORDENADAS
# ____________________________________________________________________________________________________________
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
# ____________________________________________________________________________________________________________
# Rotina para ler o arquivo original que contem os CEP´s para verificar se tem erros na sua formação
# ____________________________________________________________________________________________________________
def verifica_cep(endereco_arquivo_origem, nome_planilha):
    print('\n      --------------------------------------------------------------', 
          '\n      ROTINA PARA VERIFICAR SE TODOS OS DÍGITOS DO CEP SÃO NÚMÉRICOS',
          '\n      --------------------------------------------------------------')
    df = pd.read_excel(endereco_arquivo_origem, sheet_name=nome_planilha, index_col=None)
    linhas, colunas = informacoes(df)
    p_print = '\n      No arquivo: ' + endereco_arquivo_origem + '\n      foram encontrados ' + str(linhas) + ' registros.'
    print(p_print)
    key = True
    for i in range(0, linhas, 1):
        cep = str(df.iat[i, 1])
        cep = cep.replace('-', '')
        cep = cep.replace('.', '')
        while len(cep) < 8:
            cep = '0' + cep
        key = cep.isnumeric()
        if not key:
            p_print = ' O número que identifica o CEP ' + str(cep) + ' existe caracter não numérico, corriga antes de procurar o endereço.'
            print(p_print)
        else:
            cep_aux = cep[0:5] + '-' + cep[5:]
            df.iat[i, 1] = str(cep_aux)
    if key:
        # ____________________________________________________________________________________________________________
        # Exclui, caso exista, registros duplicados
        # ____________________________________________________________________________________________________________
        df.drop_duplicates(inplace=True)
        linhas_s, colunas = informacoes(df)
        if linhas_s != linhas:
            p_print = ' Foram excluidos ' + str(linhas_s - linhas) + ' registros duplicados.'
            print(p_print)
            linhas = linhas_s
        # ____________________________________________________________________________________________________________
        # ORGANIZA O ARQUIVO DE SAIDA PELA IDENTIFICAÇÃO DO IMÓVEL
        # ____________________________________________________________________________________________________________
        #        df.sort_values(by='Imóvel', axis=0, ascending=True, inplace=True)
        #        print('\n Os registros foram organizados em ordem ascendente pela identificação do imóvel.')
    return key, df, linhas
if __name__ == '__main__':
    inicio = time.time()
    diretorio = 'C:/Users/acmdo/Meu Drive/PYTHON_Coordenadas/INV/'
    final_arquivo = '.xlsx'

    arquivo_origem = 'Referencias_Aracaju'
    nome_planilha = 'imoveis'
    endereco_arquivo_origem = diretorio + arquivo_origem + final_arquivo
    # ____________________________________________________________________________________________________________
    #   VERIFICO SE O ARQUIVOS ORIGEM EXISTE
    # ____________________________________________________________________________________________________________
    if os.path.isfile(endereco_arquivo_origem):
        key, df, linhas = verifica_cep(endereco_arquivo_origem, nome_planilha)
        if key:
            # ____________________________________________________________________________________________________________
            # IDENTIFICA O ARQUIVO DE SAIDA OS CEP's COM A FORMAÇÃO CORRETA
            # ____________________________________________________________________________________________________________
            arquivo_ceps = arquivo_origem + '_CEP'
            endereco_arquivo_ceps = diretorio + arquivo_ceps + final_arquivo
            nome_planilha = 'ceps'
            # ____________________________________________________________________________________________________________
            # SE O ARQUIVO ONDE SERÃO SALVOS OS CEPs VERIFICADOS E FORMATADOS EXISTIR, EXCLUO PARA INICIAR AS INFORMAÇÕES
            # ____________________________________________________________________________________________________________
            if os.path.isfile(endereco_arquivo_ceps):
                try:
                    os.remove(endereco_arquivo_ceps)
                except:
                    p_print = '\n\n    **** ERRO no comando de remover o arquivo {:s} *** \n\n'.format(endereco_arquivo_ceps)
                    print(p_print)
            df_ceps = pd.DataFrame(columns=['Imóvel', 'CEP'])
            df_ceps.to_excel(endereco_arquivo_ceps, sheet_name=nome_planilha, header=True, index=False)
            for i in range(0, linhas, 1):
                imovel = df.iat[i, 0]
                cep = df.iat[i, 1]
                df_ceps.loc[i] = [imovel, cep]
            df_ceps.to_excel(endereco_arquivo_ceps, sheet_name=nome_planilha, header=True, index=False)
            p_print = ('\n      No arquivo ' + endereco_arquivo_ceps + '\n      foram gravados ' + str(linhas) + ' registros.')
            print(p_print)
            # ____________________________________________________________________________________________________________
            # SE O ARQUIVO ONDE SERÃO SALVOS OS ENDEREÇOS EXISTIR, EXCLUO PARA INICIAR AS INFORMAÇÕES
            # ____________________________________________________________________________________________________________
            arquivo_enderecos = arquivo_origem + '_ENDERECOS'
            endereco_arquivo_enderecos = diretorio + arquivo_enderecos + final_arquivo
            planilha_enderecos = 'cep_endereços'

            if os.path.isfile(endereco_arquivo_enderecos):
                try:
                    os.remove(endereco_arquivo_enderecos)
                except:
                    p_print = '\n\n    **** ERRO no comando de remover o arquivo {:s} *** \n\n'.format(endereco_arquivo_enderecos)
                    print(p_print)
            df_enderecos = pd.DataFrame(columns=['Imóvel', 'UF', 'Cidade', 'Bairro', 'Endereço', 'CEP', 'Status', 'Endereco_Completo'])
            df_enderecos.to_excel(endereco_arquivo_enderecos, sheet_name=planilha_enderecos, header=True, index=False)
            # ____________________________________________________________________________________________________________
            #   SE O ARQUIVO ONDE SERÃO SALVOS AS COORDENADAS EXISTIR, EXCLUO PARA INICIAR AS INFORMAÇÕES
            # ____________________________________________________________________________________________________________           
            arquivo_coordenadas = arquivo_origem + '_ENDERECOS_COORDENADAS'
            endereco_arquivo_coordenadas = diretorio + arquivo_coordenadas + final_arquivo
            planilha_coordenadas = 'cep_endereços_coordenadas'
            # ____________________________________________________________________________________________________________
            #   SE O ARQUIVO ONDE SERÃO SALVOS AS COORDENADAS EXISTIR, EXCLUO
            # ____________________________________________________________________________________________________________
            if os.path.isfile(endereco_arquivo_coordenadas):
                try:
                    os.remove(endereco_arquivo_coordenadas)
                except:
                    p_print = '\n\n    **** ERRO no comando de remover o arquivo {:s} *** \n\n'.format(endereco_arquivo_coordenadas)
                    print(p_print)
            df_coordenadas = pd.DataFrame(columns=['Imóvel', 'UF', 'Cidade', 'Bairro', 'Endereço', 'CEP', 'Status', 'Endereco_Completo', 'Coordenadas'])
            df_coordenadas.to_excel(endereco_arquivo_coordenadas, sheet_name=planilha_coordenadas, header=True, index=False)
            final = time.time()
            dif = final - inicio
            dias, horas, minutos, segundos, dec, cent, mile = transforma_segundos(dif)
            p_print = '\n      Tempo total de processamento: {:2} dias, {:2} horas, {:2}, minutos, {:2} segundos, {:1} décimos, {:1} centésimos, {:5f} milésimos'.format(dias, horas, minutos, segundos, dec, cent, mile)
            print(p_print)            
        else:
            p_print = '\n\n      ERRO: ENCONTREI CEP INVÁLIDO, VERIFIQUE SEUS DADOS.'
            print(p_print)
    else:
        p_print = '\n\n      ERRO: NÃO ENCONTREI O ARQUIVO: ' + endereco_arquivo_origem
        print(p_print)