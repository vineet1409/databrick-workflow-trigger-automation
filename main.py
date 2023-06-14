from auto_trigger.workflows.workflow import Workflow
from auto_trigger.tasks.data_ingestion_task import data_ingestion_task
from auto_trigger.tasks.prediction_task import prediction_task
from auto_trigger.tasks.evaluation_task import evaluation_task
from auto_trigger.configuration.constants import config
    
instance_url = config['databricks-instance']
api_token = config['databricks-token']

@data_ingestion_task
def data_ingestion(task):
    print('inside data ingestion-1')
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
    workflow = MyWorkflow()