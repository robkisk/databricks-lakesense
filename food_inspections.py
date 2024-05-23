import os, time
import requests
import json
from datetime import datetime

# Define the API response CSV storage path:
api_resp_path = "/Volumes/dbiq/food_inspections/data"

# Delete the directory:
dbutils.fs.rm(api_resp_path,True)
# Remake the dir:
dbutils.fs.mkdirs(api_resp_path)

# Define API URL:
inspection_data_url = "https://data.cityofchicago.org/resource/4ijn-s7e5.csv?$limit=200000000"

# Define the API response file name
now = datetime.now() # current date and time
fmt_now = now.strftime("%Y%m%d_%H-%M-%S")
	
# Create the empty response file:
try:
  print('---------------------------------------------------')
  print('Creating empty CSV response file.')
  dbutils.fs.put(f"{api_resp_path}/inspections_{fmt_now}.csv", "")
except:
  print('File already exists')

# Call API & populate the response file:
#----------------------------------------------------------
resp = requests.get(inspection_data_url)
if resp.status_code != 200:
    # This means something went wrong
    raise ApiError(f'GET /tasks/ {resp.status_code}')

print("Response Status Code : ", resp.status_code)
resp_csv_str = resp.content.decode("utf-8")
print("Byte size of JSON Response: ", len(resp_csv_str))
#----------------------------------------------------------
  
with open(f"{api_resp_path}/inspections_{fmt_now}.csv","w") as f:
  f.write(resp_csv_str)
  
dbutils.fs.ls(api_resp_path)

# table_location = "richardt_demos.chicago_data.food_inspections"
api_resp_path = "/Volumes/dbiq/food_inspections/data"
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(api_resp_path)
df.display()
df.write.format("delta").mode("overwrite").saveAsTable("dbiq.food_inspections.raw_data")

%sql
SELECT 
  count(*),
  min(inspection_date),
  max(inspection_date)
FROM dbiq.food_inspections.raw_data
