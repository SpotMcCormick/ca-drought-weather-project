import json
import logging
import requests
import datetime as dt
import boto3
from pathlib import Path
import yaml 

BASE_DIR = Path(__file__).resolve().parents[3]


# run this every friday
FILE_DATE = dt.datetime.now()
FILTER_DATE = FILE_DATE - dt.timedelta(days = 1)
FILE_DATE = FILE_DATE.strftime("%Y%m%d_%H%M%S")
FILTER_DATE = FILTER_DATE.strftime("%m/%d/%Y") 
START_DATE = FILTER_DATE
END_DATE = FILTER_DATE

with open(BASE_DIR / 'config/config.yaml', 'r') as file:
    config = yaml.safe_load(file)

url = config['endpoints']['us_drought_monitor']['url']
aws_bucket= config['aws']['aws_bucket']
aws_key = config['aws']['bronze_keys']['us_drought_monitor']
aws_key = aws_key.format(file_date=FILE_DATE)

logger = logging.getLogger(__name__)

drought_url = url.format(start_date=START_DATE, end_date=END_DATE)

def extract_drought_data(url):
    '''
    Description: Gets data from a url
    :param url: API endpoing url
    :return:Json data or no data (none)
    '''
    url = url
    headers = {'Accept': 'application/json'}
    logger.info(f'attempting to call the api here {url}')
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        logger.info(f'getting data from {url}')
        try:
            json_data = response.json()
            logger.info(f'got data from {url}')
            return json_data

        except requests.exceptions.JSONDecodeError:
            logger.error(f"can't reach {url}")
            return None
    else:
        logger.error(f'API call failed from {url}', exc_info=True)
        return None

def upload_to_s3(json_data):
    '''
    Description: Upload data to AWS bucket
    :param json_data:  the data collected from the API endpoint
    :return: Json data into s3 bucket or no data (none)
    '''
    try:
        logger.info(f"communicating with {aws_bucket}")
        s3 = boto3.client('s3')

        s3.put_object(
            Body=json.dumps(json_data),
            Bucket= aws_bucket,
            Key=aws_key
            )
        logger.info(f"loaded to {aws_bucket}/{aws_key}")
        return True
    except Exception as e:
        logger.error(f'error wth {e}', exc_info=True)
        return False




#run
if __name__== "__main__":
    data = extract_drought_data(drought_url)
    if data:
        try:
            upload_to_s3(data)
        except Exception as e:
            print (f'{e}')
            raise
