
# CADASTRO DE PRODUTO 
# EXTRAÇÃO DE DADAOS DA PLATAFORMA DE CHAMADOS SERVICE DESK PLUS VIA API
# MANIPULAÇÃO DOS DADOS / VALIDAÇÃO VIA BANCO DE DADOS ORACLE
# INCLUSÃO DO CADASTRO DE PRODUTO VIA API WEBSERVICE
# AUTHOR : Luan Patrick Garcia Machado
# Data : 21/01/2024


# BIBLIOTECAS 
import cx_Oracle
import csv
import pandas as pd
import requests 
import re
import sys
import json
import re
import time
import logging
import urllib3
from unidecode import unidecode
from requests.packages.urllib3.exceptions import InsecureRequestWarning


urllib3.disable_warnings(InsecureRequestWarning)


logging.getLogger("urllib3").setLevel(logging.WARNING)

# Configurar o logger
logging.basicConfig(filename='logfile.txt', level=logging.DEBUG)

# Adicionar um manipulador de console para imprimir logs no console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)  # Configurar para o nível desejado
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console_handler.setFormatter(formatter)

# Adicionar o manipulador ao logger
logging.getLogger().addHandler(console_handler)

# Exemplo de logs coloridos
logging.info("\033[92mNota enviada com sucesso.\033[0m")  # Verde
logging.error("\033[91mFalha ao enviar a nota.\033[0m")  # Vermelho

lib_dir=r"C:\instantclient_21_8"
cx_Oracle.init_oracle_client(lib_dir=lib_dir)

connection = cx_Oracle.connect(user="", password='',
                            dsn=""
                            )   


logging.basicConfig(level=logging.DEBUG)

#Função responsável pelo envio das requisições POST
def send_request(url, headers, filtro):
    input_data = "input_data=" + str(filtro)
    response = requests.post(url, headers=headers, params=input_data, verify=False)
    
    if response.status_code == 200 or response.status_code == 201:
        return "Requisição enviada com sucesso."
    else:
        logging.error(f"Falha ao enviar a requisição. Código de status: {response.status_code}")
        logging.error(f"Detalhes do erro: {response.text}")
        return f"Falha ao enviar a requisição. Código de status: {response.status_code}"

#Função responsável pelo envio das requisições PUT
def send_put_request(url, headers, filtro):
    input_data = "input_data=" + str(filtro)
    response = requests.put(url, headers=headers, params=input_data, verify=False)

    if response.status_code == 200 or response.status_code == 201:
        return "Requisição PUT enviada com sucesso."
    else:
        logging.error(f"Falha ao enviar a requisição PUT. Código de status: {response.status_code}")
        logging.error(f"Detalhes do erro: {response.text}")
        return f"Falha ao enviar a requisição PUT. Código de status: {response.status_code}"

#Função responsável pelo envio das notas de comunicação
def send_note_request(url, headers, filtro):
    input_data = "input_data=" + str(filtro)
    response = requests.post(url, headers=headers, params=input_data, verify=False)
    
    if response.status_code == 200 or response.status_code == 201:
        logging.info("Nota enviada com sucesso.")
        
    else:
        logging.error(f"Falha ao enviar a nota. Código de status: {response.status_code}")
        logging.error(f"Detalhes do erro: {response.text}")

#Função responsável pela execução das consultas SQL
def execute_sql_query(cursor, query, params=None):
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    
    results = cursor.fetchone()
    if results:
        return results
    else:
        return []

#Funções responsável pela limpesa dos dados da API Servicedesk
def clean_and_process_field(field_value):
    if field_value is None:
        return ""

    cleaned_value = field_value
    if isinstance(field_value, list):
        cleaned_value = ' '.join(str(item) for item in field_value)


    cleaned_value = cleaned_value.encode('utf-8').decode('utf-8')
    cleaned_value = str(cleaned_value).replace("['", "").replace("']", "").replace("kg", "")

    return cleaned_value

def processar_dados_solicitante(cod_produto, cod_similar, ncm):
    cod_produto = clean_and_process_field(cod_produto)
    cod_similar = clean_and_process_field(cod_similar)
    ncm = re.sub(r'[^a-zA-Z0-9\s/-]', '', ncm).upper()

    return cod_produto, cod_similar, ncm

# Url API Servicedesk
url = "https://accounts.zoho.com/oauth/v2/token?refresh_token=1000.38de536aaba542bd8167214a65dfa6c4.9c9790274c88348d5d325b239331ee43&grant_type=refresh_token&client_id=1000.A3IDPZJ28CXQA3CX5FGCCI9LM52QMZ&client_secret=2c46713402e697d9e434d3c70bdfe31ca441232d4f&redirect_uri=https://suporte..com/app/itdesk&scope=SDPOnDemand.requests.ALL"

# Faz o post para pedir o token 
response = requests.post(url)

if response.status_code == 200:
    try:
        resp = response.json()
        token_variavel = resp['access_token']
    except json.JSONDecodeError:
        print("Error decoding JSON response.")
        sys.exit()
else:
    sys.exit("Failed to obtain token.")


#URL para trazer todos os chhamados
url_variavel= f'https://suporte..com/app/cscdesk/api/v3/requests'

#campos para pedir a requisição na url + o token variavel 
headers = {
            "Authorization": f"Zoho-oauthtoken {token_variavel}",
            "Accept": "application/vnd.manageengine.v3+json",
}

# Filtro para buscar no service desk   
filtro = {
    "list_info": {
    "row_count": 100,
    
    "search_criteria": [
    {
        "field": "subcategory.name",
        "condition": "is",
        "logical_operator": "and",
        "values": [
            "Produto"
        ]
    },
    {
        "field": "status.name",
        "condition": "is",
        "logical_operator": "and",
        "values": [
        "RPA - 1"
        ]
    },
    ]
}
}


input_data = "input_data=" + str(filtro)

# juntando a url
response = requests.get(url_variavel,headers=headers,params=input_data,verify=False)

resposta_id = response.json()

if not resposta_id['requests']:
    print("nao foi encontrado nenhum chamado")
    sys.exit()

# Função responsável por formatar os campos extraidos do Servicedesk
def clean_field(field_value):
    if field_value is None:
        return ""
    elif isinstance(field_value, list):
        cleaned_value = ' '.join(str(item) for item in field_value)
    else:
        cleaned_value = field_value

    cleaned_value = str(cleaned_value).encode('utf-8').decode('utf-8').replace("['", "").replace("']", "").replace("kg", "")
    return cleaned_value

# Função responsável por validar os campos não atribuídos
def validar_campos_nao_atribuidos(linha, sistema_principal, tipo_prod):
    if not linha:
        linha = "801"
    if not sistema_principal:
        sistema_principal = "9990"
    if not tipo_prod:
        tipo_prod = "ME"

    return linha, sistema_principal, tipo_prod

# Função responsável por processar os chamados e extrais os campos que estão dentro da lista fields_to_clean
def processar_chamado(variavel_id, headers):
    variavel = variavel_id['id']
    url_variavel_id = f'https://suporte..com/app/cscdesk/api/v3/requests/{variavel}'
    print(url_variavel_id)
    
    # Obter detalhes do chamado
    response_id = requests.get(url_variavel_id, headers=headers)
    campos = response_id.json()
    
    # Lista de campos a serem limpos
    fields_to_clean = [
        'udf_char699', 'udf_char700', 'udf_char702', 'udf_char347', 'udf_char320',
        'udf_char347', 'udf_char704', 'udf_char705', 'udf_char706', 'udf_char707',
        'udf_char708', 'udf_long10', 'udf_char709', 'udf_char710', 'udf_char132',
        'udf_char319', 'udf_char713', 'udf_char322', 'udf_char297', 'udf_char296',
        'udf_char321', 'udf_char701'
    ]

    # Dicionário para armazenar os valores dos campos
    field_values = {}

    # Loop para percorrer os nomes dos campos
    for field in fields_to_clean:
        # Remove o prefixo "udf_char"
        field_name = field.replace("udf_char", "")
        
        # Limpa o valor do campo usando a função clean_field
        cleaned_value = clean_field(campos['request']['udf_fields'][field])
        
        # Adiciona o nome do campo e o valor ao dicionário
        field_values[field_name] = cleaned_value

    return variavel, field_values

dados_solicitacao = []  

cursor = connection.cursor()

for variavel_id in resposta_id['requests']:
    dados_solicitacao = []  
    itens = []
    itens_f = []
    itens_grp = []
    intes_cod_p = []
    itens_vSmilar = []
    itens_ncm = []

    variavel, field_values = processar_chamado(variavel_id, headers)


    cod_similar = clean_field(field_values.get('699', ""))
    cod_produto = clean_field(field_values.get('700', ""))
    categoria = clean_field(field_values.get('702', "")[:2])
    unid_medida = clean_field(field_values.get('320', "")[:2])
    desc_prod = clean_field(field_values.get('701', ""))
    marca = clean_field(field_values.get('347', ""))
    peso_bruto = clean_field(field_values.get('704', ""))
    peso_liquido = clean_field(field_values.get('705', ""))
    altura = clean_field(field_values.get('706', ""))
    largura = clean_field(field_values.get('707', ""))
    comprimento = clean_field(field_values.get('708', ""))
    qtde_usada_maquina = clean_field(field_values.get('udf_long10', ""))
    qtde_por_embalagem = clean_field(field_values.get('709', ""))
    importado = clean_field(field_values.get('710', "")[:1])
    ncm = clean_field(field_values.get('132', ""))
    origem = clean_field(field_values.get('319', "")[:1])
    subgrupos = clean_field(field_values.get('713', "")[:4])
    linha = clean_field(field_values.get('296', "")[:3])
    sistema_principal = clean_field(field_values.get('322', "")[:4])
    tipo_equipamento = clean_field(field_values.get('297', "")[:5])
    tipo_prod = clean_field(field_values.get('321', "")[:2])

    linha, sistema_principal, tipo_prod = validar_campos_nao_atribuidos(linha, sistema_principal, tipo_prod)

    # Crie um dicionário para armazenar os valores dos campos
    fields = {
        'cod_similar': cod_similar,
        'cod_produto': cod_produto,
        'categoria': categoria,
        'unid_medida': unid_medida,
        'desc_prod': desc_prod,
        'marca': marca,
        'peso_bruto': peso_bruto,
        'peso_liquido': peso_liquido,
        'altura': altura,
        'largura': largura,
        'comprimento': comprimento,
        'qtde_usada_maquina': qtde_usada_maquina,
        'qtde_por_embalagem': qtde_por_embalagem,
        'importado': importado,
        'ncm': ncm,
        'origem': origem,
        'subgrupos': subgrupos,
        'linha': linha,
        'sistema_principal': sistema_principal,
        'tipo_equipamento': tipo_equipamento,
        'tipo_prod': tipo_prod,
        'ipi_prod': 0
    }

    #funções responsável por validar campos do servicedesk no banco de dados Oracle
    def validar_campos(cursor, cod_produto, subgrupos):


        if not cod_produto:
            return "O campo cod_produto não pode estar vazio."
        
        if not ncm:
            return "O campo NCM não pode estar vazio"

        # Verificar se o código do produto já existe na tabela SB1010
        query = "SELECT RTRIM(B1_COD), RTRIM(B1_DESC), B1_GRUPO, RTRIM(B1_FABRIC), B1_TIPCAT, B1_POSIPI FROM TOTVS.SB1010 WHERE RTRIM(B1_COD) = :cod"
        resultado = execute_sql_query(cursor, query, {"cod": cod_produto})

        if resultado:
            return f"Código a ser cadastrado já existe. Código: {resultado[0]} Descrição: {resultado[1]}"
        
        query = "SELECT RTRIM(YD_TEC), RTRIM(YD_DESC_P), RTRIM(YD_UNID), RTRIM(YD_PER_IPI) FROM TOTVS.SYD010 WHERE RTRIM(YD_TEC)=:ncm"
        resultado = execute_sql_query(cursor, query, {"ncm": ncm})

        if not resultado:
            return f"A NCM mencionada no chamado não está cadastrada no cadastro de NCM. Favor entrar em contato com o setor fiscal."
            

        query = "SELECT * FROM TOTVS.SBM010 WHERE BM_GRUPO =: subgrupo"
        resultado = execute_sql_query(cursor, query, {"subgrupo": subgrupos})

        if not resultado:
            return f"O subgrupo não está cadastrado na tabela de Grupos de Produtos, será necessário efetuar o cadastro no configurador"

        return None

    def v_grtrib (cursor, ncm):

        query = "SELECT RTRIM(B1_GRTRIB) FROM TOTVS.SB1010 WHERE RTRIM(B1_POSIPI)=:cod AND ROWNUM = 1 ORDER BY B1_GRTRIB"
        v_grtrib = execute_sql_query(cursor, query, {"cod": ncm})

        return v_grtrib

    def v_similar_master(cursor, cod_similar):
        query = "SELECT RTRIM(ZY_PROD), RTRIM(ZY_DESC), RTRIM(ZY_MASTER) FROM TOTVS.SZY010 WHERE RTRIM(ZY_PROD)=:similar_f and d_e_l_e_t_ = '  '"
        v_master = execute_sql_query(cursor, query, {"similar_f": cod_similar})
        print(v_master)

        if v_master and len(v_master) > 2:
            
            v_master = v_master[2]
        else:
            v_master = None

        return v_master

    #função responsável por validar a primeira análise antes de enviar para a API Produto
    def requests_sdp(variavel, token_variavel, headers, validacao_result):
        itens_sdp = []

        if validacao_result:
            itens_sdp.append(validacao_result)
            print(itens_sdp)

        url_variavel_notes = f'https://suporte..com/app/cscdesk/api/v3/requests/{variavel}/notes'
        logging.error(f"Chamado: Erro ao cadastrar produto: {validacao_result}")

        if itens_sdp:
            mensagem = "\n".join(itens_sdp)
            print(mensagem)
            logging.info(f"Mensagem a ser enviada: {mensagem}")
            mensagem = "\n".join(itens_sdp)
            filtro = {
                "request_note": {
                    "mark_first_response": False,
                    "add_to_linked_requests": False,
                    "notify_technician": True,
                    "show_to_requester": False,
                    "description": mensagem
                }
            }
            note_result = send_note_request(url_variavel_notes, headers, filtro)
            itens_sdp.clear()

            url_variavel_put= f'https://suporte..com/app/cscdesk/api/v3/requests/{variavel}'


            headers = {
                "Authorization": f"Zoho-oauthtoken {token_variavel}",
                "Accept": "application/vnd.manageengine.v3+json",
            }

            url_variavel_put = f'https://suporte..com/app/cscdesk/api/v3/requests/{variavel}'
            
            filtro_aberto = {
                "request": {
                    "status": {
                        "id": "168405000000006657",
                    }
                }
            }

            input_data = "input_data=" + str(filtro_aberto)
            response = requests.put(url_variavel_put,headers=headers,params=input_data,verify=False)
    # Função responsável por comunicar o retorno da API de Produto Webservice
    def requests_product_api(variavel, token_variavel, headers, json_response):
        itens_api = []
        url_variavel_notes = f'https://suporte..com/app/cscdesk/api/v3/requests/{variavel}/notes'

        if "Produto" in json_response:
            itens_api.append("Produto cadastrado com sucesso!")
            produto_info = json_response["Produto"][0] if json_response["Produto"] else {}

            # Obtém os valores das chaves
            codigo_produto = produto_info.get("Codigo Produto", "")
            descricao = produto_info.get("Descricao", "")

            # Adiciona as informações formatadas à lista
            itens_api.append(f"Detalhes do Produto:")
            itens_api.append(f"Código do Produto: {codigo_produto}")
            itens_api.append(f"Descrição: {descricao}")

            if itens_api:
                mensagem = "\n".join(itens_api)
                filtro = {
                    "request_note": {
                        "mark_first_response": False,
                        "add_to_linked_requests": False,
                        "notify_technician": True,
                        "show_to_requester": True,
                        "description": mensagem
                    }
                }
                send_note_request(url_variavel_notes, headers, filtro)
                itens_api.clear()
                dados_solicitacao.clear()

                # URL para trazer todos os chamados
                url_variavel_put = f'https://suporte..com/app/cscdesk/api/v3/requests/{variavel}'

                # Campos para pedir a requisição na URL + o token variável
                headers = {
                    "Authorization": f"Zoho-oauthtoken {token_variavel}",
                    "Accept": "application/vnd.manageengine.v3+json",
                }

                filtro_fechar = {
                    "request": {
                        "status": {
                            "id": "168405000002772493",
                        }
                    }
                }

                input_data = "input_data=" + str(filtro_fechar)
                response = requests.put(url_variavel_put, headers=headers, params=input_data, verify=False)

        else:
            itens_api.append("Erro ao cadastrar produto:")
            itens_api.append(str(json_response))
            logging.error(f"Chamado: Erro ao cadastrar produto: {json.dumps(json_response)}")


            if itens_api:
                mensagem = "\n".join(itens_api)
                filtro = {
                    "request_note": {
                        "mark_first_response": False,
                        "add_to_linked_requests": False,
                        "notify_technician": True,
                        "show_to_requester": False,
                        "description": mensagem
                    }
                }
                send_note_request(url_variavel_notes, headers, filtro)
                itens_api.clear()
                dados_solicitacao.clear()

                # URL para trazer todos os chamados
                url_variavel_put = f'https://suporte..com/app/cscdesk/api/v3/requests/{variavel}'

                # Campos para pedir a requisição na URL + o token variável
                headers = {
                    "Authorization": f"Zoho-oauthtoken {token_variavel}",
                    "Accept": "application/vnd.manageengine.v3+json",
                }

                filtro_fechar = {
                    "request": {
                        "status": {
                            "id": "168405000000006657",
                        }
                    }
                }

                input_data = "input_data=" + str(filtro_fechar)
                response = requests.put(url_variavel_put, headers=headers, params=input_data, verify=False)

        response_put = requests.post(url)

        # Se a resposta for verdadeira, volta e traz em forma de texto
        if response_put.status_code == 200:
            resposta_put = json.loads(response_put.text)
            token_variavel = resposta_put['access_token']

        else:
            logging.warning(f" response_put == 400: Não foi possível trazer o token_variavel,")

        if itens_api:
            mensagem = "\n".join(itens_api)
            filtro = {
                "request_note": {
                    "mark_first_response": False,
                    "add_to_linked_requests": False,
                    "notify_technician": False,
                    "show_to_requester": False,
                    "description": mensagem
                }
            }

            note_result = send_note_request(url_variavel_notes, headers, filtro)
            itens_api.clear()
            dados_solicitacao.clear()


    validacao_result = validar_campos(cursor, cod_produto, subgrupos)

    if validacao_result is not None:
        
        requests_sdp(variavel, token_variavel, headers, validacao_result)

        breakpoint
        
    else:

        grtrib_t = v_grtrib(cursor, ncm)
        v_similar_master = v_similar_master(cursor, cod_similar)

        variables = {
            'ncm': ncm, 
            'altura': altura, 
            'largura': largura, 
            'comprimento': comprimento,
            'categoria': categoria, 
            'desc_prod': desc_prod, 
            'subgrupos': subgrupos, 
            'unid_medida': unid_medida,
            'tipo_equipamento': tipo_equipamento, 
            'sistema_principal': sistema_principal, 
            'linha': linha, 
            'tipo_prod': tipo_prod,
            'origem': origem, 
            'cod_produto': cod_produto, 
            'marca': marca,
            'qtde_usada_maquina': qtde_usada_maquina,
            'peso_liquido': peso_liquido,
            'peso_bruto': peso_bruto,
            'qtde_por_embalagem': qtde_por_embalagem
        }



        dados_para_enviar = {
            "Produto": {
                "B1_COD": cod_produto,
                "B1_DESC": desc_prod,
                "B1_TIPO": tipo_prod,
                "B1_UM": unid_medida,
                "B1_GRUPO": subgrupos,
                "B1_LOCPAD": "01",
                "B1_CODBAR": cod_produto,
                "B1_CODITE": cod_produto,
                "B1_LINHA": linha,
                "B1_POSIPI": ncm,
                "B1_QUMQ": float(qtde_usada_maquina),  
                "B1_PESO": float(peso_liquido.replace(',', '.')),  
                "B1_PESBRU": float(peso_bruto.replace(',', '.')),  
                "B1_TIPCAT": categoria,
                "B1_QE": float(qtde_por_embalagem),  
                "B1_FABRIC": marca,
                "B1_IMPORT": importado,
                "B1_ORIGEM": origem,
                "B1_MSBLQL": "2",
                "B1_MASTER": v_similar_master if v_similar_master else " ",
                "B1_GRTRIB": grtrib_t[0] if grtrib_t else "P10",
                "B1_SUBGRP": sistema_principal if sistema_principal != "Não atribuído" else " ",
                "B1_EX_NCM": " ",
                "B1_IPI": 0,
                "B1_CEST": " ",
                "B1_CONTA": " "
            },
            "Autentica": {
              
            }
        }

        #base prod
        url_servico = "http://:####/rest/Produto"

        dados_solicitacao.append(dados_para_enviar)
        print(dados_solicitacao)
        for dados_para_enviar in dados_solicitacao:
            max_tentativas = 3  
            tentativa_atual = 1

            while True:
                response_servico = requests.post(url_servico, json=dados_para_enviar)
                json_response = response_servico.json()

                if isinstance(json_response, dict) and json_response.get('message') in ['Nao existe licenca disponivel no License Server para atender a requisicao nesse momento.', 'Precondition Required']:
                    logging.warning(f"Tentativa {tentativa_atual}: Aguardando 15 segundos antes de tentar novamente...")
                    time.sleep(15)
                    tentativa_atual += 1
                else:
                    # Chama a função requests_product_api com os parâmetros necessários
                    requests_product_api(variavel, token_variavel, headers, json_response)
                    break

                if tentativa_atual > max_tentativas:
                    continue


            

        





