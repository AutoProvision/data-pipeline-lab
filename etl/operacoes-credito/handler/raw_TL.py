import zipfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import numpy as np
import boto3

s3 = boto3.client('s3')

bucket_source = 'autoprovision-datalake-staging'
bucket_dest = 'autoprovision-datalake-raw'
prefix_source = 'banco-central/operacoes-credito'

def dataframefy(f):
	df = pd.read_csv(f, sep=';', encoding='utf-8-sig')
	INDEXADOR_MAP_TABLE = {
		'Prefixado': 'Pré-fixado',
		'Outros indexadores': np.nan,
		'Índices de preços': np.nan,
	}
	df['indexador_modalidade'] = df['indexador'].replace(INDEXADOR_MAP_TABLE)
	df.drop(
		columns=[
      		'cnae_subclasse',
        	'a_vencer_ate_90_dias',
         	'a_vencer_de_91_ate_360_dias',
         	'a_vencer_de_361_ate_1080_dias',
         	'a_vencer_de_1081_ate_1800_dias',
         	'a_vencer_de_1801_ate_5400_dias',
         	'a_vencer_acima_de_5400_dias',
         	'vencido_acima_de_15_dias',
			'tcb',
			'sr',
			'indexador',
			'origem',
        ],
		inplace=True
	)
	return df

def lambda_handler(event, context):
	YEAR = event['year']
	MONTH = event['month']

	zip_key = f'{prefix_source}/{YEAR}/planilha.zip'

	print(f'Processando arquivo: {zip_key}')

	zip_obj = s3.get_object(Bucket=bucket_source, Key=zip_key)
	zip_data = zip_obj['Body'].read()
	zip_file = zipfile.ZipFile(BytesIO(zip_data))

	with zip_file as z:
		with z.open(f'planilha_{YEAR}{MONTH}.csv') as f:
			df = dataframefy(f)

			parquet_buffer = BytesIO()
			pq.write_table(pa.Table.from_pandas(df), parquet_buffer)
			parquet_buffer.seek(0)

			parquet_key = f'{prefix_source}/{YEAR}{MONTH}.parquet'
			s3.upload_fileobj(parquet_buffer, bucket_dest, parquet_key)
			print(f'Arquivo {parquet_key} enviado com sucesso')
