from auto_trigger.tasks.base_task import Task
from auto_trigger.logger.logger import create_logger
from auto_trigger.configuration.constants import config

logger = create_logger()


def data_ingestion_task(task_func):
    def wrapper(instance_url, api_token):
        job_id = config['data_ingestion_job_id']
        print('inside wrapper of data-ingestion')
        task = Task(instance_url, api_token, job_id)
        k = task_func(task)
        return k
    return wrapper