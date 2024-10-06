import json, io, httpx
from bs4 import BeautifulSoup
from zipfile import ZipFile
import boto3

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    response = httpx.get('https://dadosabertos.bcb.gov.br/dataset/scr_data')
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, 'html.parser')

    main_ul = soup.find('ul', class_='resource-list')

    li_list = main_ul.find_all('li', class_='resource-item')

    zip_url = None
    for li in li_list:
        if li is not None:
            zip_url = li.find('a', class_='resource-url-analytics').get('href')
    
    year = zip_url[len(zip_url) - 8: len(zip_url) - 4]

    response = httpx.get(zip_url)
    response.raise_for_status()
    zip_content = io.BytesIO(response.content)

    csv_url = ''
    month = ''

    with ZipFile(zip_content, 'r') as zip_file:
        csv_url = zip_file.namelist()[-1]
        month = csv_url[len(csv_url) - 6: len(csv_url) - 4]

    bucket_name = 'autoprovision-datalake-staging'
    prefix = 'banco-central/operacoes-credito/'
    
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')

    directories = []
    if 'CommonPrefixes' in response:
        directories = [obj['Prefix'] for obj in response['CommonPrefixes']]

    if f'{prefix}{year}-{month}/' in directories:
        return {
            'statusCode': 200,
            'body': json.dumps('Diretório desse mês já existe no bucket')
        }

    s3_key = f'{prefix}{year}-{month}/operacoes-credito.zip'
    s3_client.upload_fileobj(zip_content, bucket_name, s3_key)

    return {
        'statusCode': 200,
        'body': json.dumps('Upload realizado com sucesso no S3!')
    }
