import json
import logging
import requests
import datetime as dt
import boto3
from pathlib import Path
import yaml 

BASE_DIR = Path(__file__).resolve().parents[3]

# this had for to run every friday since the map updates every thursday
FILE_DATE = dt.datetime.now()
FILTER_DATE = FILE_DATE - dt.timedelta(days = 1)
FILE_DATE = FILE_DATE.strftime("%Y%m%d_%H%M%S")
FILTER_DATE = FILTER_DATE.strftime("%m/%d/%Y") 
START_DATE = FILTER_DATE
END_DATE = FILTER_DATE

with open(BASE_DIR / 'config.yaml', 'r') as file:
    config = yaml.safe_load(file)

url = config['us_drought_monitor']['url']
aws_bucket= config['aws']['aws_bucket']
aws_key = config['aws']['bronze_keys']['us_drought_monitor']
aws_key = aws_key.format(file_date=FILE_DATE)

drought_url = url.format(start_date=START_DATE, end_date=END_DATE)


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler(BASE_DIR / 'logs/initial-load-drought.log'),
        logging.StreamHandler()
    ]
)

def extract_drought_data(url):
    '''
    Description: Gets data from a url
    :param url: API endpoing url
    :return:Json data or no data (none)
    '''
    url = url
    headers = {'Accept': 'application/json'}
    logging.info(f'attempting to call the api here {url}')
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        logging.info(f'getting data from {url}')
        try:
            json_data = response.json()
            logging.info(f'got data from {url}')
            return json_data

        except requests.exceptions.JSONDecodeError:
            logging.error(f"can't reach {url}")
            return None
    else:
        logging.error(f'API call failed from {url}', exc_info=True)
        return None

def upload_to_s3(json_data):
    '''
    Description: Upload data to AWS bucket
    :param json_data:  the data collected from the API endpoint
    :return: Json data into s3 bucket or no data (none)
    '''
    try:
        logging.info(f"communicating with {aws_bucket}")
        s3 = boto3.client('s3')

        s3.put_object(
            Body=json.dumps(json_data),
            Bucket= aws_bucket,
            Key=aws_key
            )
        logging.info(f"loaded to {aws_bucket}/{aws_key}")
        return True
    except Exception as e:
        logging.error(f'error wth {e}', exc_info=True)
        return False




#example usage
if __name__== "__main__":
    data = extract_drought_data(drought_url)
    if data:
        try:
            upload_to_s3(data)
            print("example usage worked!")
        except Exception as e:
            print (f'{e}')
    else:
        print ("example usage failed!")
