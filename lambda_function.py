import json
import os
import boto3
import time
from datetime import datetime

vLocalTimeZone = 'America/Sao_Paulo'


def lambda_handler(event, context):
    # print(f"Event -> {event}")
    # print(f"Context -> {context}")
    os.environ['TZ'] = vLocalTimeZone
    time.tzset()
    now = datetime.now().strftime("%Y%m%d%H%M%S")
    print(f"Horario Atual -> {now}")

    # Obtém o nome do arquivo de controle
    body_event = json.loads(event['Records'][0]['body'])
    print(f"body_event -> {body_event}")

    bucket_event_records_s3 = body_event['Records'][0]['s3']
    print(f"bucket_event_records_s3 -> {bucket_event_records_s3}")

    bucket_event_name = bucket_event_records_s3['bucket']['name']
    print(f"bucket_event_name -> {bucket_event_name}")

    file_name = bucket_event_records_s3['object']['key']
    print(f"file_name -> {file_name}")

    # Extrai os 11 primeiros dígitos do arquivo de controle
    prefix = file_name[:11]
    print(f"Prefix -> {prefix}")

    sufix = "DIARIO.TXT"
    print(f"Sufix -> {sufix}")

    # Configurações do AWS S3
    s3_client = boto3.client('s3')
    bucket_name = bucket_event_name

    # Caminho para o diretório de validação
    validation_directory = 'validado/'
    error_directory = 'error/'

    # Lista para armazenar registros válidos e inválidos
    valid_records = []
    invalid_records = []

    # Obtém a lista de objetos no mesmo bucket do arquivo de controle
    objects = s3_client.list_objects_v2(Bucket=bucket_name)['Contents']
    print(f"Objects -> {objects}")

    # Filtra os objetos que possuem o mesmo prefixo
    relevant_objects = [obj for obj in objects if obj['Key'].startswith(prefix) and objects if
                        obj['Key'].endswith(sufix)]
    print(f"Relevant_objects -> {relevant_objects}")

    # Itera sobre os objetos relevantes
    print(f"Vou iniciar o for do relevant_objects")
    for obj in relevant_objects:
        # Obtém o conteúdo do arquivo
        response = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
        content = response['Body'].read().decode('utf-8')

        # Verifica se o conteúdo é uma data válida
        try:
            datetime.strptime(content, '%Y-%m-%d')
            valid_records.append(content)
        except ValueError:
            invalid_records.append(content)

    # Gera o nome do arquivo de saída
    output_file_name = file_name.replace('CONTROLE', 'DIARIO').split('.')[0] + f"_{now}.TXT"
    print(f"Nome do arquivo de saida -> {output_file_name}")

    # Caminho para o diretório de saída
    output_directory = os.path.join(validation_directory, output_file_name)
    print(f"Caminho para o diretorio de saida -> {output_directory}")

    # Gera o arquivo de saída com os registros válidos
    s3_client.put_object(Body='\n'.join(valid_records), Bucket=bucket_name, Key=output_directory)
    print(f"Arquivo {output_directory} salvo com sucesso no bucket {bucket_name}")

    # Gera o arquivo de erro com os registros inválidos
    error_file_name = file_name.replace('CONTROLE', 'ERROR').split('.')[0] + f"_{now}.TXT"
    output_error_directory = os.path.join(error_directory, error_file_name)
    s3_client.put_object(Body='\n'.join(invalid_records), Bucket=bucket_name, Key=output_error_directory)
    print(f"Error_file_name -> {error_file_name}")
    print(f"Arquivo {output_error_directory} salvo com sucesso no bucket {bucket_name}")
