## This script gets data from NOAA, stores it in file '/Users/chuckschultz/work/data/dump.xlsx' and logs the transaction
## Variables needed for api call:
##   classif: (list)type of disasters - concatenate using +
##   iso: (list)countries of disasters - concatenate using +
##   from: (int)start date - 1900 to 2021
##   to: (int)end date - 1900 to 2021

import requests
import datetime
noaa_token=''


# Set variables
base_url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
dataset_id = "?datasetid=GHCND"
data_types = []
locations = []
stations = []




# Extract NOAA data
def get_noaa_data():
  try:
    #noaa_token = os.environ['noaa_token']
    header = {'token': noaa_token}
    url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&locationid=ZIP:80455&startdate=2020-01-01&enddate=2020-01-31&limit=10"
    r = requests.get(url, headers=header)
    print(r.content)
  except:
    print('You fucked something up!')
    traceback.print_exc()

get_noaa_data()
