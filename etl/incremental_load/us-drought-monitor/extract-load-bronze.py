import json
import logging
from this import d
import requests
import datetime as dt
import boto3
from pathlib import Path

root_dir = Path(__file__).parent.parent.parent.parent

print(root_dir)

# logging.basicConfig(
#     format='%(asctime)s %(levelname)-8s %(message)s',
#     level=logging.INFO,
#     handlers=[
#         logging.FileHandler(root_dir / 'logs/initial-load-drought.log'),
#         logging.StreamHandler()
#     ]
# )
# # this had for to run every friday since the map updates every thursday
# filter_date = dt.datetime.now() - dt.timedelta(days = 1)
# filter_date = filter_date.strftime("%m/%d/%Y") 
# print(filter_date)





# aws_bucket = 'drought-data-lake'
# aws_key = f'drought-us-monitor/initial-load/bronze/data/{file_date}_data.json'


# def extract_drought_data(url):
#     '''
#     Description: Gets data from a url
#     :param url: API endpoing url
#     :return:Json data or no data (none)
#     '''
#     url = url
#     headers = {'Accept': 'application/json'}
#     logging.info(f'attempting to call the api here {url}')
#     response = requests.get(url, headers=headers)

#     if response.status_code == 200:
#         logging.info(f'getting data from {url}')
#         try:
#             json_data = response.json()
#             logging.info(f'got data from {url}')
#             return json_data

#         except requests.exceptions.JSONDecodeError:
#             logging.error(f"can't reach {url}")
#             return None
#     else:
#         logging.error(f'API call failed from {url}', exc_info=True)
#         return None

# def upload_to_s3(json_data):
#     '''
#     Description: Upload data to AWS bucket
#     :param json_data:  the data collected from the API endpoint
#     :return: Json data into s3 bucket or no data (none)
#     '''
#     try:
#         logging.info(f"communicating with {aws_bucket}")
#         s3 = boto3.client('s3')

#         s3.put_object(
#             Body=json.dumps(json_data),
#             Bucket= aws_bucket,
#             Key=aws_key
#             )
#         logging.info(f"loaded to {aws_bucket}/{aws_key}")
#         return True
#     except Exception as e:
#         logging.error(f'error wth {e}', exc_info=True)
#         return False




# #example usage
# if __name__== "__main__":
#     url = f"https://usdmdataservices.unl.edu/api/CountyStatistics/GetDSCI?aoi=CA&startdate=1/1/2000&enddate={end_date}&statisticsType=1"

#     data = extract_drought_data(url)
#     if data:
#         try:
#             upload_to_s3(data)
#             print("example usage worked!")
#         except Exception as e:
#             print (f'{e}')
#     else:
#         print ("example usage failed!")
