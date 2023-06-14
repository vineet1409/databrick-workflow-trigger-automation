from setuptools import setup

setup(
    name='auto_trigger',
    version='1.0',
    author='Dana-Farber Cancer Institute',
    packages=['auto_trigger', 'auto_trigger.logger', 'auto_trigger.workflows', 'auto_trigger.tasks',\
              'auto_trigger.configuration'],
    install_requires=[
        'requests'
    ]
)