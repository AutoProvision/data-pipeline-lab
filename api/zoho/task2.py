import pandas as pd
import random
import numpy as np
from faker import Faker
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
import boto3
import io

s3_client = boto3.client('s3')

def gerar_cnae_nota():
    df_cnae = pd.read_excel("cnaes_tratados.xlsx")
    return df_cnae.sample(n=1)[['CNAE', 'DESCRIÇÃO', 'NOTA_CNAE']].iloc[0].to_dict()

def gerar_nome_sobrenome():
    fake = Faker()
    nome = fake.first_name() 
    sobrenome = fake.last_name() 
    return f'{nome} {sobrenome}'

def gerar_dados_sinteticos(num_amostras):
    def gerar_cpf():
        return ''.join(random.choices('0123456789', k=11))

    def gerar_modalidade() -> tuple:
        notas_modalidades = {
            "CAPITAL_GIRO_PRAZO_ATE_365_DIAS": 1.0,
            "CREDITO_PESSOAL_NAO_CONSIGNADO": 0.8,
            "CAPITAL_GIRO_PRAZO_SUPERIOR_365_DIAS": 0.3,
            "CONTA_GARANTIDA": 0.8,
            "CHEQUE_ESPECIAL": 0.6,
            "CREDITO_PESSOAL_CONSIGNADO_PRIVADO": 0.3,
            "DESCONTO_DUPLICATAS": 0.8,
            "AQUISICAO_VEICULOS": 0.6,
            "CARTAO_CREDITO_ROTATIVO_TOTAL": 0.3,
            "CREDITO_PESSOAL_CONSIGNADO_PUBLICO": 0.3,
            "AQUISICAO_OUTROS_BENS": 0.3,
            "CARTAO_CREDITO_PARCELADO": 0.8,
            "CREDITO_PESSOAL_CONSIGNADO_INSS": 0.3,
            "ADIANTAMENTO_SOBRE_CONTRATOS_CAMBIO": 0.6,
            "DESCONTO_CHEQUES": 0.8,
            "VENDOR": 1.0,
            "ANTECIPACAO_FATURAS_CARTAO_CREDITO": 1.0,
            "ARRENDAMENTO_MERCANTIL_VEICULOS": 1.0,
            "FINANCIAMENTO_IMOBILIARIO_TAXAS_MERCADO": 1.0,
            "FINANCIAMENTO_IMOBILIARIO_TAXAS_REGULADAS": 0.6
        }

        chave_aleatoria = random.choice(list(notas_modalidades.keys()))

        return chave_aleatoria, notas_modalidades[chave_aleatoria]

    dados = []
    for _ in range(num_amostras):
        cnae_object = gerar_cnae_nota()

        nome = gerar_nome_sobrenome()
        cpf = gerar_cpf()
        valor = round(random.uniform(1000, 20000), 2)
        cnae = cnae_object['CNAE']
        descricao_cnae = cnae_object['DESCRIÇÃO']
        modalidade, peso_modalidade = gerar_modalidade()
        peso_cnae = cnae_object['NOTA_CNAE']
        peso_historico = random.random()
        parcelas_restantes = round(random.randint(1, 24), 2)
        qtd_emprestimo_ativos = round(random.randint(1, 5), 2) 
        vl_carteira_pagar = valor * parcelas_restantes 
        vl_renda = round(random.uniform(1000, 20000), 2)
        peso_extra = 0

        if vl_renda * parcelas_restantes <= valor * parcelas_restantes:
            peso_extra = 0.5

        dados.append([nome, cpf, valor, cnae, peso_cnae, descricao_cnae, modalidade, peso_modalidade, vl_carteira_pagar, parcelas_restantes, qtd_emprestimo_ativos, peso_historico, vl_renda, peso_extra])

    df = pd.DataFrame(dados, columns=['nome', 'cpf', 'valor', 'cnae', 'peso_cnae', 'descricao_cnae', 'modalidade', 'peso_modalidade', 'vl_carteira_inadimplida', 'parcelas_restantes', 'qtd_emprestimo_ativos', 'peso_historico', 'vl_renda', 'peso_extra'])
    return df

def lambda_handler(event, context):
    df = gerar_dados_sinteticos(2000)

    dados = df[['peso_extra', 'peso_cnae', 'peso_historico', 'vl_carteira_pagar']]

    scaler = MinMaxScaler()
    dados_normalizados = scaler.fit_transform(dados)
    dados_normalizados = pd.DataFrame(dados_normalizados, columns=dados.columns)

    modelo_kmeans = KMeans(n_clusters=12, random_state=45, n_init = 'auto')
    modelo_kmeans.fit(dados_normalizados)

    dados_analise = pd.DataFrame()
    dados_analise[dados_normalizados.columns] = dados_normalizados
    dados_analise[dados_normalizados.columns] = scaler.inverse_transform(dados_normalizados)

    dados_analise['cluster'] = modelo_kmeans.labels_
    cluster_media = dados_analise.groupby('cluster').mean()
    cluster_media.T

    cluster_classificado = dados_analise.groupby('cluster').mean()

    cluster_classificado['media_geral'] = cluster_classificado.mean(axis=1)

    cluster_classificado_sorted = cluster_classificado.sort_values(by='media_geral')

    n_clusters = len(cluster_classificado_sorted)
    tercio = n_clusters // 3

    cluster_classificado_sorted['risco'] = np.where(cluster_classificado_sorted.index <= tercio, 'Alto', np.where(cluster_classificado_sorted.index <= 2 * tercio, 'Médio', 'Baixo'))

    cluster_classificado = dados_analise.merge(cluster_classificado_sorted[['risco']], left_on='cluster', right_index=True, how='left')
    cluster_classificado = cluster_classificado.merge(df, left_index=True, right_index=True, how='left')

    for column in cluster_classificado:
        if str(column).endswith('_y'):
            cluster_classificado.drop(columns=column, inplace=True)
        if str(column).endswith('_x'):
            cluster_classificado.rename(columns={column : column.replace('_x','')}, inplace=True)

    cluster_classificado['provisionamento'] = cluster_classificado['risco'].apply(
        lambda x: 0.10 if x == 'Alto' else (0.05 if x == 'Médio' else 0.02)
    )

    colunas_originais = ['nome','cpf','valor','cnae','descricao_cnae','modalidade','parcelas_restantes','qtd_emprestimo_ativos','cluster','provisionamento','vl_carteira_pagar','vl_renda','peso_extra','peso_cnae','peso_historico','risco']

    print(cluster_classificado.dtypes)

    buffer = io.BytesIO()
    cluster_classificado[colunas_originais].to_excel(buffer, index=False, index_label=False)
    buffer.seek(0)
    s3_client.upload_fileobj(buffer, 'datalake-credito', 'banco-central/operacoes-credito/resultado_modelo.xlsx')

    buffer = io.BytesIO()
    cluster_classificado[colunas_originais].to_csv(buffer, index=False)
    buffer.seek(0)
    s3_client.upload_fileobj(buffer, 'datalake-credito', 'banco-central/operacoes-credito/resultado_modelo.csv')

def handler():
    return lambda_handler({}, {})
