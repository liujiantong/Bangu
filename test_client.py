#!/usr/bin/env python
# coding: utf-8

import time
from banbu_client import BanguClient


if __name__ == '__main__':
    params = {
        "name": "pi computer",
        "executor-cores": "2",
        "num-executors": 2
    }

    client = BanguClient('http://172.172.172.165:8998')

    py = '/Users/liutao/workspace/spark-solr-etl/pi.py'
    ret_json = client.submit(py, params=params)
    print('submit response: %s' % ret_json)

    job_id = ret_json.get('job_id', None)
    # job_id = '7ef71c08-f195-43df-b644-cb27d759a21e'

    while True:
        job_json = client.get_job(job_id)
        print('job response: %s' % job_json)

        status = job_json.get('status', None)
        print('job:%s %s' % (job_id, status))

        if status == u'finished' or status == u'failed':
            break
        else:
            time.sleep(5)


