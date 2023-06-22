from auto_trigger.tasks.base_task import Task
from auto_trigger.logger.logger import create_logger
from auto_trigger.configuration.constants import config
from auto_trigger.logger.mlflow_logger import log_to_mlflow

logger = create_logger()

def evaluation_task(task_func):
    def wrapper(instance_url, api_token):
        job_id = config['evaluation_task_job_id']
        log_to_mlflow(params={'evaluation_jobid':job_id})
        task = Task(instance_url, api_token, job_id)
        k = task_func(task)
        return k
    return wrapper
