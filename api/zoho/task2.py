import pandas as pd
import random
from faker import Faker

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
        print(chave_aleatoria)

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
    print(df.head(20))

def handler():
    return lambda_handler({}, {})
