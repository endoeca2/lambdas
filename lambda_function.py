import boto3
import json
from EBX import EBX
import pandas as pd
from AWS import AWS
import os

dynamodb = boto3.resource('dynamodb')
tabla_estatus = os.getenv('tabla_estatus')
tabla_dynamo = dynamodb.Table(tabla_estatus)
s3 = boto3.client('s3')

OUTPUT_JSON_PATH = 'api/catalogo-autos/carga-continua/map-input/fragments_success.json'

def lambda_handler(event, context):
    try:
        ruta_modelo_datos = os.getenv("ruta_modelo_datos")
        ebx = EBX(ruta_modelo_datos)
        s3_bucket_name = os.getenv("s3_bucket_name")
        aws_conn = AWS()

        vista_safe = ebx.select_vista_content('ENTIDAD_VEHICULO', 'ENTIDAD_SAFE')
        aws_conn.guardar_dataframe_vista_en_s3(vista_safe, 'VISTA_SAFE_ENTIDAD_VEHICULO.csv', s3_bucket_name)

        # Obtener UUID
        uuid = event['UUID']

        # Consultar DynamoDB
        items = query_dynamodb(uuid, "SUCCESS|TARIFA", tabla_dynamo)

        if not items:
            return {
                'statusCode': 200,
                'body': 'No hay registros exitosos para procesar.',
                'UUID': uuid
            }

        registros = [{"registro": item["registro"], "UUID": item["UUID"]} for item in items]

        s3.put_object(
            Bucket=s3_bucket_name,
            Key=OUTPUT_JSON_PATH,
            Body=json.dumps(registros)
        )

        return {
            'statusCode': 200,
            'message': 'JSON con registros guardado en S3 exitosamente y ENTIDAD_SAFE cargada.',
            's3_path': OUTPUT_JSON_PATH,
            'UUID': uuid
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Error procesando datos: {str(e)}"
        }

def query_dynamodb(uuid, resultado, tabla, page_limit=None):
    items = []
    last_evaluated_key = None
    while True:
        params = {
            'KeyConditionExpression': "#uuid = :uuid",
            'FilterExpression': "resultado = :result",
            'ExpressionAttributeNames': {"#uuid": "UUID"},
            'ExpressionAttributeValues': {":uuid": uuid, ":result": resultado},
            'Limit': page_limit or 1000
        }
        if last_evaluated_key:
            params['ExclusiveStartKey'] = last_evaluated_key

        response = tabla.query(**params)
        items.extend(response['Items'])
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break

    return items
