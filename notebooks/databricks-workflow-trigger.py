import requests
import json

# Define the Databricks instance URL and API token
instance_url = 'adb-2589727608526365.5.azuredatabricks.net'
api_token = 'dapi6f23733ff1285223d5e9bb4ce72f8eb2-3'

# Define the job ID
job_id = '786623175995094'

# Trigger the Databricks job
url = f'https://{instance_url}/api/2.0/jobs/run-now'
headers = {
    'Authorization': f'Bearer {api_token}',
    'Content-Type': 'application/json'
}

payload = {
    'job_id': int(job_id),
    'timeout_seconds': 3600
}

response = requests.post(url, headers=headers, data=json.dumps(payload))

if response.status_code == 200:
    print('Job triggered successfully!')
else:
    print('Failed to trigger job:', response.text)