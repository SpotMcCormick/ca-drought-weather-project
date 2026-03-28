import logging
import requests
import datetime as dt
import boto3
from pathlib import Path

root_dir = Path(__file__).parent.parent.parent.parent.parent

#logging config
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    filename= root_dir / "logs/ca-gis-county-log",
    level=logging.INFO
)
 #used for filename
file_date = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

#aws config
aws_bucket = 'drought-data-lake'
aws_key = f'gis-ca-county/initial-load/bronze/data/{file_date}_data.zip'

#extract function
def extract_ca_county_data(url):
    '''
    Description: Gets data from a api
    :param url: API endpoint url
    :return:Json data or no data
    '''
    url = url
    logging.info(f'attempting to call the api here {url}')
    response = requests.get(url)

    if response.status_code == 200:
        logging.info(f'getting data from {url}')
        try:
            data = response.content
            logging.info(f'got data from {url}')
            return data

        except Exception as e:
            logging.error(f"error on {e}")
            return None
    else:
        logging.error(f'API call failed from {url}', exc_info=True)
        return None

#upload function
def upload_to_s3(data):
    '''
    Description: Upload data to AWS bucket
    :param json_data:  the data collected from the API endpoint
    :return: Uploaded json data into s3 bucket or no data
    '''
    try:
        logging.info(f"communicating with {aws_bucket}")
        s3 = boto3.client('s3')

        s3.put_object(
            Body=data,
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
    url = "https://data.ca.gov/dataset/e212e397-1277-4df3-8c22-40721b095f33/resource/b0007416-a325-4777-9295-368ea6b710e6/download/ca_counties.zip"

    data = extract_ca_county_data(url)
    if data:
        try:
            upload_to_s3(data)
            print("example usage worked!")
        except Exception as e:
            print (f'{e}')
    else:
        print ("example usage failed!")