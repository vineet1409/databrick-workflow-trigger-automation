from auto_trigger.configuration.constants import config
from time import sleep

class Workflow:
    def __init__(self):
        self.tasks = []

    def trigger(self, instance_url, api_token):
        for task in self.tasks:
            print('\n')
            print(task)
            ret = task(instance_url, api_token)
            if ret == 'success':
                print('task is successful..!!')
                continue
            else:
                print('breaking...!!')
                break

    def add_task(self, task_func):
        self.tasks.append(task_func)