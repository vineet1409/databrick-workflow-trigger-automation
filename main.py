from auto_trigger.workflows.workflow import Workflow
from auto_trigger.tasks.data_ingestion_task import data_ingestion_task
from auto_trigger.tasks.prediction_task import prediction_task
from auto_trigger.tasks.evaluation_task import evaluation_task
from auto_trigger.configuration.constants import config
from auto_trigger.configuration.create_config_enviornment import create_enviornment
from auto_trigger.logger.mlflow_logger import log_to_mlflow

import argparse


instance_url = config['databricks-instance']
api_token = config['databricks-token']

log_to_mlflow(params={"instance_url": instance_url, "api_token": api_token})


@data_ingestion_task
def data_ingestion(task):
    flag = task.execute()
    return flag

@prediction_task
def prediction(task):
    flag = task.execute()
    return flag

@evaluation_task
def evaluation(task):
    flag = task.execute()
    return flag


class MyWorkflow(Workflow):
    def __init__(self):
        self.tasks = []
        self.add_task(data_ingestion)
        self.add_task(prediction)
        self.add_task(evaluation)
        self.trigger(instance_url,api_token)
        

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--update_cluster", action="store_true", help="Update cluster configuration")
    args = parser.parse_args()
    
    create_enviornment(instance_url,api_token,args)
    workflow = MyWorkflow()