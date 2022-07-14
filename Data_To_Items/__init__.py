import traceback
import logging
logging.info('Importing packages...')
import azure.functions as func
import json
import requests
import base64
import time
import datetime


class PredefinedError(Exception):
    def __init__(self, message):
        self.message = message
        super(PredefinedError, self).__init__(message)


def create_range_list(init_date, end_date, the_range):
    start = datetime.datetime.strptime(init_date, "%Y-%m-%d")
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    days = int(the_range)
    days = datetime.timedelta(days=days)

    dates_list = []
    end_temp_date = start
    init_temp_date = end_temp_date
    end_temp_date = end_temp_date + days

    while end_temp_date <= end:
        
        if end_temp_date >= end:
            dates_list.append(f"'{init_temp_date.strftime('%Y-%m-%d')}' AND '{end.strftime('%Y-%m-%d')}'")
        else:
            dates_list.append(f"'{init_temp_date.strftime('%Y-%m-%d')}' AND '{end_temp_date.strftime('%Y-%m-%d')}'")
        init_temp_date = end_temp_date + datetime.timedelta(days=1)
        end_temp_date = end_temp_date + days + datetime.timedelta(days=1)
    
    return dates_list


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Getting query...')
    post_query = req.get_json()
    initdate = post_query.get('initdate')
    enddate = post_query.get('enddate')
    the_range = post_query.get('range')

    start_time = time.time()


    elapsed_time = time.time() - start_time

    # raise PredefinedError('Job Timeout...')
    # thestr = str([f"'{initdate}' AND '{enddate}'", "'2020-05-01' AND '2020-05-01'"])

    thestr = create_range_list(initdate, enddate, the_range)

    the_result_list = "{" + f"'values': {thestr}" + "}"
    return str(the_result_list)

    # except Exception as e:
        # error = traceback.format_exc()
        # return str("{'FinalState': " + f"'{error}'" + '}')  # Error para visualización en desarrollo
        # return str(e)  # Error para visualización de fronend