#!/usr/bin/env python
# coding: utf-8

import requests


class BanguClient(object):

    def __init__(self, host):
        self.host = host

    def submit(self, py, py_zip=None, params=None):
        files = {
            'py': open(py, "rb"),
        }
        if py_zip:
            files['py-files'] = open(py_zip, "rb")

        try:
            r = requests.post(self.host + '/submit', data=params, files=files)
            return r.json()
        except:
            return {}

    def get_jobs(self):
        try:
            r = requests.post(self.host + '/jobs')
            return r.json()
        except:
            return []

    def get_job(self, job_id):
        try:
            r = requests.post('%s/job/%s' % (self.host, job_id))
            return r.json()
        except:
            return {}

