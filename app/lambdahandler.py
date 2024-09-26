import json
import logging
import boto3

# Usando o logger padrão do Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Inicializando o cliente boto3 para Glue
glue = boto3.client('glue')
s3 = boto3.client('s3')

def handler(event, context):
    # Log do evento recebido
    logger.info(f'Event recebido: {event}')

    # Extraindo informações do bucket
    s3_event = event['Records'][0]['s3']
    bucket_name = s3_event['bucket']['name']

    # Logando informações do bucket
    logger.info(f'Bucket: {bucket_name}')

    # Listar os objetos na raiz do bucket
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)

        # Verifica se existem objetos no bucket
        if 'Contents' not in response:
            logger.warning('O bucket está vazio.')
            return {
                'statusCode': 404,
                'body': json.dumps('Nenhum arquivo encontrado no bucket.')
            }

        # Encontrar o primeiro arquivo JSON ou CSV na raiz do bucket
        valid_file = next(
            (obj['Key'] for obj in response['Contents']
             if obj['Key'].count('/') == 0 and (obj['Key'].endswith('.json') or obj['Key'].endswith('.csv'))),
            None
        )

        if not valid_file:
            logger.warning('Nenhum arquivo JSON ou CSV encontrado na raiz do bucket.')
            return {
                'statusCode': 404,
                'body': json.dumps('Nenhum arquivo JSON ou CSV encontrado na raiz do bucket.')
            }

        logger.info(f'Arquivo encontrado: {valid_file}')

        # Iniciando o Glue Job, passando os argumentos necessários
        glue_response = glue.start_job_run(
            JobName='parquet-conversion-job',
            Arguments={
                '--input_path': f's3://{bucket_name}/{valid_file}',  # Caminho completo do arquivo
                '--output_path': f's3://{bucket_name}/parquet-output/'  # Local de saída dos arquivos Parquet
            }
        )
        logger.info(f'Glue Job iniciado com sucesso: {glue_response["JobRunId"]}')
    except Exception as e:
        logger.error(f'Erro ao listar ou iniciar o Glue Job: {str(e)}')
        return {
            'statusCode': 500,
            'body': json.dumps('Erro ao processar o bucket.')
        }

    return {
        'statusCode': 200,
        'body': json.dumps('Glue Job Started!')
    }