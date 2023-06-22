import requests
import json
from time import sleep
from auto_trigger.logger.mlflow_logger import log_to_mlflow

class Task:
    def __init__(self, instance_url, api_token,job_id):
        self.instance_url = instance_url
        self.api_token = api_token
        self.job_id = job_id
        print('self.job_id is',self.job_id)
        

    def execute(self):
        print("Executing task...")
        
        # Trigger the Databricks job
        url = f'https://{self.instance_url}/api/2.0/jobs/run-now'
        headers = {
            'Authorization': f'Bearer {self.api_token}',
            'Content-Type': 'application/json'
        }

        payload = {
            'job_id': int(self.job_id),
            'timeout_seconds': 3600
        }

        response = requests.post(url, headers=headers, data=json.dumps(payload))

        if response.status_code == 200:
            print(f'Job {self.job_id} triggered successfully!')

            '''run_id = response.json()['run_id']
            run_status_url = f'https://{self.instance_url}/api/2.0/jobs/runs/get?run_id={run_id}'

            while True:
                run_status_response = requests.get(run_status_url, headers=headers)
                run_status = run_status_response.json()['state']['life_cycle_state']

                if run_status == 'TERMINATED':
                    print(f'Job {self.job_id} completed!')
                    return 'success'

                sleep(25)  # Adjust the polling interval as needed'''
            
            log_to_mlflow(params={'job-id':self.job_id,'status':'success'}) 
               
            return 'success'

        else:
            print(f'Failed to trigger job {self.job_id}:', response.text)
            
