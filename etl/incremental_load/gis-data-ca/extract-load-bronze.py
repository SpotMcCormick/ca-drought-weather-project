import logging
import requests
import datetime as dt
import boto3
from pathlib import Path
import yaml

BASE_DIR = Path(__file__).resolve().parents[3]

with open(BASE_DIR / 'config/config.yaml', 'r') as file:
    config = yaml.safe_load(file)

logger = logging.getLogger(__name__)
 #used for filename
file_date = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

#aws config
aws_bucket= config['aws']['aws_bucket']
aws_key = config['aws']['bronze_keys']['gis_ca_county']
aws_key = aws_key.format(file_date=file_date)


#extract function
def extract_ca_county_data(url):
    '''
    Description: Gets data from a api
    :param url: API endpoint url
    :return:Json data or no data
    '''
    url = url
    logger.info(f'attempting to call the api here {url}')
    response = requests.get(url)

    if response.status_code == 200:
        logger.info(f'getting data from {url}')
        try:
            data = response.content
            logger.info(f'got data from {url}')
            return data

        except Exception as e:
            logger.error(f"error on {e}")
            return None
    else:
        logger.error(f'API call failed from {url}', exc_info=True)
        return None

#upload function
def upload_to_s3(data):
    '''
    Description: Upload data to AWS bucket
    :param json_data:  the data collected from the API endpoint
    :return: Uploaded json data into s3 bucket or no data
    '''
    try:
        logger.info(f"communicating with {aws_bucket}")
        s3 = boto3.client('s3')

        s3.put_object(
            Body=data,
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
    url = config['endpoints']['ca_gis']['url']
    data = extract_ca_county_data(url)
    if data:
        try:
            upload_to_s3(data)
        except Exception as e:
            print (f'{e}')
    else:
        raise