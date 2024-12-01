import boto3
import pandas as pd
from io import BytesIO
import os
from decimal import Decimal, ROUND_HALF_UP

s3_client = boto3.client('s3')

TODAY = pd.Timestamp.today().strftime('%Y-%m-%d')
SRC_BUCKET_NAME = os.getenv("BUCKET_RAW_NAME")
SRC_PATH = f'banco-central/juros-bancarios/{TODAY}/df.parquet'
DEST_BUCKET_NAME = os.getenv("BUCKET_TRUSTED_NAME")
DEST_PATH = f'banco-central/juros-bancarios/{TODAY}/df.parquet'

def transform_taxa(valor_taxa, periodo_desejado, periodo_atual):
    vt = Decimal(valor_taxa)
    pd = Decimal(periodo_desejado)
    pa = Decimal(periodo_atual)

    a = 1 + (vt / 100)
    b = pd / pa
    c = a ** b
    d = (c - 1) * 100

    return d

def trim_decimal(n):
    valor = Decimal(str(n).rstrip('0').rstrip('.'))
    return valor.quantize(Decimal('1.0000'), rounding=ROUND_HALF_UP)

def lambda_handler(event, context):
    obj = s3_client.get_object(Bucket=SRC_BUCKET_NAME, Key=SRC_PATH)
    df = pd.read_parquet(BytesIO(obj['Body'].read()), engine='pyarrow')
    del obj

    df['data'] = pd.to_datetime(df['data'], format='%Y-%m-%d')

    df['TaxaJurosAoAno'] = df['TaxaJurosAoAno'].str.replace(',', '.').astype(float)

    df['SubClassificacao'] = df['Modalidade'].apply(lambda x: x.split(' - ')[-1]).astype('category')
    df['Modalidade'] = df['Modalidade'].apply(lambda x: x.rsplit(' - ', 1)[0]).astype('category')

    df['TaxaJurosAoMes'] = df.apply(lambda x: trim_decimal(transform_taxa(x['TaxaJurosAoAno'], 1, 12)), axis=1).astype(float)

    SUBCLASSIFICACAO_REPLACE_MAP = {
        'PÓS-FIXADO': 'Pós-fixado',
        'PRÉ-FIXADO': 'Pré-fixado',
        'PÓS-FIXADO REFERENCIADO EM IPCA': 'Pós-fixado referenciado em IPCA',
        'PÓS-FIXADO REFERENCIADO EM TR': 'Pós-fixado referenciado em TR',
    }
    df['SubClassificacao'] = df['SubClassificacao'].replace(SUBCLASSIFICACAO_REPLACE_MAP).astype('category')

    MODALIDADE_REPLACE_MAP = {
        'FINANCIAMENTO IMOBILIÁRIO COM TAXAS DE MERCADO': 'Financiamento imobiliário com taxas de mercado',
        'FINANCIAMENTO IMOBILIÁRIO COM TAXAS REGULADAS': 'Financiamento imobiliário com taxas reguladas',
    }
    df['Modalidade'] = df['Modalidade'].replace(MODALIDADE_REPLACE_MAP).astype('category')

    MODALIDADE_REPLACE_MAP = {
        'Pessoa Física': 'PF',
        'Pessoa Jurídica': 'PJ',
    }
    df['Segmento'] = df['Segmento'].replace(MODALIDADE_REPLACE_MAP).astype('category')

    df.rename(columns={
        'Segmento': 'ct_classificacao',
        'Modalidade': 'ct_modalidade',
        'InstituicaoFinanceira': 'ct_instituicao_financeira',
        'SubClassificacao': 'ct_indexador_modalidade',
        'TaxaJurosAoAno': 'vl_taxa_juros_ano',
        'TaxaJurosAoMes': 'vl_taxa_juros_mes',
        'data': 'dt_referencia',
    }, inplace=True)

    output_buffer = BytesIO()
    df.to_parquet(output_buffer, index=False, engine='pyarrow')
    del df

    s3_client.put_object(Bucket=DEST_BUCKET_NAME, Key=DEST_PATH, Body=output_buffer.getvalue())
    del output_buffer

def handler():
    return lambda_handler({}, {})
