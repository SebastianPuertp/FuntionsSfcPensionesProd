 # ***************************************
 # Nombre del proyecto: Superfinanciera
 # Nombre del script: CallSuperIngest
 # Nombre de la empresa: Woombat
 # Desarrollador: Joaquin Alonso Jimenez Caviedes 'Crackio'
 # Fecha modificacion: 2020/11/10/
 # Modificaciones: Joaquin Alonso Jimenez Caviedes 'Crackio'
 # Fecha modificaciones: 2020/11/10
 # En este programa dispara un evento de encendido Databricks
 # Para correrlo: Sólo hay que llamar el Endpoint
 # JAJC
 # ***************************************


import traceback
import logging
logging.info('Importing packages...')
import azure.functions as func
import json
import requests
import base64
import time

DOMAIN = ''
TOKEN = ''
# BASE_URL = f'https://{DOMAIN}/api/2.0/clusters/'
BASE_URL = f'https://{DOMAIN}/api/2.0/'
CLUSTER_ID = ''
JOB_ID = '8'
CLUSTER_TIME = 600
JOB_TIME = 200


class PredefinedError(Exception):
    def __init__(self, message):
        self.message = message
        super(PredefinedError, self).__init__(message)


def dbfs_API_call(action, API, body, HttpMethod='POST'):
    """ request/response is encoded/decoded as JSON
    https://{DOMAIN}/api/2.0/clusters/get?cluster_id={CLUSTER_ID}/'
    https://{DOMAIN}/api/2.0/jobs/run-now?job_id={JOB_ID}/'
    """
    if HttpMethod == 'POST':
        response = requests.post(
                                    BASE_URL + API + '/' + action,
                                    headers={'Authorization': f'Bearer {TOKEN}'},
                                    json=body
                                )
    else:
        response = requests.get(
                                    BASE_URL + API + '/' + action,
                                    headers={'Authorization': f'Bearer {TOKEN}'},
                                    json=body
                                )
    return response.json()


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('Starting Cluster...')
        start_time = time.time()
        dbfs_API_call('start', 'clusters', {"cluster_id": CLUSTER_ID})

        logging.info('Getting Cluster state...')
        cluster_state = dbfs_API_call('get', 'clusters', {'cluster_id': CLUSTER_ID}, 'GET')['state']
        elapsed_time = time.time() - start_time

        while(cluster_state != 'RUNNING' and elapsed_time < CLUSTER_TIME):
          cluster_state = dbfs_API_call('get', 'clusters', {'cluster_id': CLUSTER_ID}, 'GET')['state']
          logging.info(f"The Cluster state is: '{cluster_state}'")
          time.sleep(15)
          elapsed_time = time.time() - start_time
        logging.info('Activación de Databricks exitosa...')


        if cluster_state == 'RUNNING':
            logging.info('Cluster iniciado con éxito...')

        elif elapsed_time >= CLUSTER_TIME:
            logging.info('Cluster Timeout...')
            raise PredefinedError('Cluster Timeout...')

        logging.info('Starting Job...')
        start_time = time.time()
        run_id = dbfs_API_call('run-now', 'jobs', {'job_id': JOB_ID})['run_id']

        logging.info('Getting Job state...')
        job_state = dbfs_API_call('runs/get', 'jobs', {'run_id': run_id}, 'GET')['state']['life_cycle_state']

        elapsed_time = time.time() - start_time

        while(job_state != 'TERMINATED' and  elapsed_time < JOB_TIME):
          job_state = dbfs_API_call('runs/get', 'jobs', {'run_id': run_id}, 'GET')['state']['life_cycle_state']
          logging.info(f"The Job state is: '{job_state}'")
          time.sleep(15)
          elapsed_time = time.time() - start_time

        if job_state == 'TERMINATED':
            logging.info('Job ejecutado con éxito...')
            return func.HttpResponse("{'FinalState': 'Job ejecutado con éxito...'}")

        elif elapsed_time >= JOB_TIME:
            logging.info('Job Timeout...')
            raise PredefinedError('Job Timeout...')


    except Exception as e:
        error = traceback.format_exc()
        return str("{'FinalState': " + f"'{error}'" + '}')  # Error para visualización en desarrollo
        # return str(e)  # Error para visualización de fronend