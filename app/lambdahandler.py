import json
import logging
import boto3


logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client('glue')
s3 = boto3.client('s3')

def handler(event, context):
    logger.info(f'Event {event}')

    s3_event = event['Records'][0]['s3']
    bucket_name = s3_event['bucket']['name']

    logger.info(f'Bucket: {bucket_name}')

    try:
        response = s3.list_objects_v2(Bucket=bucket_name)

        if 'Contents' not in response:
            logger.warning('Empty bucket.')
            return {
                'statusCode': 404,
                'body': json.dumps('No file found in the bucket.')
            }

        valid_file = next(
            (obj['Key'] for obj in response['Contents']
             if obj['Key'].count('/') == 0 and (obj['Key'].endswith('.json') or obj['Key'].endswith('.csv'))),
            None
        )

        if not valid_file:
            logger.warning('No JSON or CSV file found')
            return {
                'statusCode': 404,
                'body': json.dumps('No JSON or CSV file found')
            }

        logger.info(f'File: {valid_file}')

        glue_response = glue.start_job_run(
            JobName='parquet-conversion-job',
            Arguments={
                '--input_path': f's3://{bucket_name}/{valid_file}',
                '--output_path': f's3://{bucket_name}/parquet-output/'
            }
        )
        logger.info(f'Glue Job started: {glue_response["JobRunId"]}')
    except Exception as e:
        logger.error(f'Error to start Glue Job: {str(e)}')
        return {
            'statusCode': 500,
            'body': json.dumps('Erro to process')
        }