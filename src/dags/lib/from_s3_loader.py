import boto3

AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

session = boto3.session.Session()

def fetch_s3_file(bucket, file_name, file_path):
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    ) 

    s3_client.download_file(
        Bucket=bucket,
        Key=file_name,
        Filename=f'{file_path}{file_name}')

    print(f'Файл {file_name} загружен !!!')

