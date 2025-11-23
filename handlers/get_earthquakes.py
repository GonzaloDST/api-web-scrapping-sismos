import json
import boto3
import os
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['EARTHQUAKES_TABLE'])

def lambda_handler(event, context):
    try:
        # Escanear la tabla para obtener todos los sismos
        response = table.scan()
        earthquakes = response.get('Items', [])
        
        # Ordenar por timestamp (más recientes primero)
        earthquakes.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        
        # Limitar si se especifica en query parameters
        query_params = event.get('queryStringParameters') or {}
        limit = query_params.get('limit')
        if limit and limit.isdigit():
            earthquakes = earthquakes[:int(limit)]
        else:
            earthquakes = earthquakes[:10]  # Default: últimos 10
        
        response_body = {
            'sismos': earthquakes,
            'total': len(earthquakes),
            'timestamp': earthquakes[0]['timestamp'] if earthquakes else None
        }
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(response_body, ensure_ascii=False)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': f'Error obteniendo sismos: {str(e)}'
            })
        }