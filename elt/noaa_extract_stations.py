## This script gets 118,487 stations data from NOAA,
## stores it in file '/Users/chuckschultz/work/data/station_dump.json' and logs the transaction
import requests
import datetime
import json
noaa_token='yGrMiddFDpDUvEPTGrOUtSAdnEoQiWFD'


# Set variables
base_url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/stations"
dataset_id = "?datasetid=GHCND"

# Function gets NOAA station data (118,487 entries - 25 at a time),
# store in file '/Users/chuckschultz/work/data/station_dump.json' and
# log transaction in file '/Users/chuckschultz/work/data/noaa_stations.log'
def get_noaa_stations():
  off = 1
  try:
    for batch in range(2): # range(4740)
      offset = "&offset=" + str(off)
      header = {'token': noaa_token}
      url = base_url + dataset_id + offset
      dump = requests.get(url, headers=header)
      
      with open('/Users/chuckschultz/work/data/station_dump.json', 'wb') as file: # store data in file
        file.write(dump.content)
      
      with open('/Users/chuckschultz/work/data/station_dump.json', 'r') as file: # read data to get metadata
        json_data = json.loads(file.read())
        print("Link: " + url + "\nCount: " + str(json_data["metadata"]["resultset"]["count"]))
        
        with open('/Users/chuckschultz/work/data/noaa_stations.log', 'a') as file: # log transaction in file
          file.write(str(datetime.datetime.now()) + "\nLink: " + url + "\nCount: " + str(json_data["metadata"]["resultset"]["count"]) + "\n")
      off += 25
  
  except TypeError: # If there are no results
    print("Count:", None)
    with open('/Users/chuckschultz/work/data/noaa_stations.log', 'a') as file: # log transaction in file
        file.write(str(datetime.datetime.now()) + "\nCount: None")

get_noaa_stations()
