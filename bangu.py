#!/usr/bin/env python
# coding: utf-8

import os
import sys
import signal
import logging

import uuid
import time
import json
import argparse

from collections import namedtuple

import subprocess
import threading
import multiprocessing

import traceback
import sqlite3

from flask import Flask, request
from flask_uploads import UploadSet, configure_uploads


SparkJob = namedtuple('SparkJob', 'job_id, name, status, py_file, start_time, end_time')

ALLOWED_EXTENSIONS = ['py', 'zip']

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 32 * 1024 * 1024

current_work_dir = os.path.dirname(os.path.abspath(__file__))
app.config['UPLOADED_FILES_DEST'] = current_work_dir
app.config['UPLOADED_FILES_ALLOW'] = ALLOWED_EXTENSIONS

job_files = UploadSet('files', extensions=ALLOWED_EXTENSIONS)
configure_uploads(app, job_files)


@app.route('/submit', methods=['POST'])
def submit_job():
    if request.method == 'POST':
        job_name = request.form['name']
        n_cores, n_exec = request.form['executor-cores'], request.form['num-executors']
        logging.info('job_name: %s, executor-cores: %s, num-executors: %s', job_name, n_cores, n_exec)

        if 'py' not in request.files:
            return 'py-file required', 400

        py_job = job_files.save(request.files['py'], folder=job_files_path)
        logging.info('py: %s', py_job)

        job_name = job_name or py_job
        n_cores = n_cores or "1"
        n_exec = n_exec or "2"

        job_args = list()
        job_args.append('spark2-submit')
        job_args.append('--name')
        job_args.append(job_name)
        job_args.append('--executor-cores')
        job_args.append(n_cores)
        job_args.append('--num-executors')
        job_args.append(n_exec)

        zip_file = None
        if 'py-files' in request.files:
            zip_file = job_files.save(request.files['py-files'], folder=job_files_path)
            logging.info('py-files: %s', zip_file)

        if zip_file:
            job_args.append('--py-files')
            job_args.append(zip_file)
        job_args.append(py_job)

        logging.info('job_args: %s', job_args)
        job_id = str(uuid.uuid4())
        job_q.put((job_id, job_args))

        result = {
            'job_id': job_id,
            'name': job_name,
            'status': 'running',
        }
        return json.dumps(result), 200
    else:
        return 'Method Not Allowed', 405


@app.route('/jobs', methods=['GET', 'POST'])
def get_jobs():
    ret_jobs = _jobs_from_store()
    logging.debug('jobs size:%d in job_store', len(ret_jobs))
    job_list = [job._asdict() for job in ret_jobs]
    return json.dumps(job_list), 200


@app.route('/job/<job_id>', methods=['GET', 'POST'])
def get_job_by_id(job_id=None):
    job = _job_byid_from_store(job_id)
    ret_json = '{}' if not job else json.dumps(job._asdict())
    return ret_json, 200


def _init_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # work_dir, _ = os.path.split(os.path.abspath(__file__))
    log_dir = current_work_dir + '/logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    console_handler = logging.StreamHandler()
    logfile_name = '%s/%s' % (log_dir, 'bangu.log')
    file_handler = logging.FileHandler(logfile_name)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


def _run_spark_job(job_id, job_args):
    proc = subprocess.Popen(job_args, stdout=subprocess.PIPE)

    job_name = job_args[job_args.index('--name') + 1]
    py_job = job_args[-1]

    _store_job(SparkJob(
        job_id=job_id,
        name=job_name,
        py_file=py_job,
        start_time=time.time(),
        status='running',
        end_time=0,
    ))

    # out, err = proc.communicate()
    # return out.decode('utf-8')
    job_result_q.put((job_id, proc))


def _do_work():
    while True:
        try:
            job_id, job_args = job_q.get()
            _run_spark_job(job_id, job_args)
        except Exception as e:
            logging.error('run_spark_job error:%s, detail: %s', e, traceback.format_exc())


def _check_work_result():
    while True:
        try:
            job_id, proc = job_result_q.get()
            retcode = proc.poll()
            job = _job_byid_from_store(job_id)
            if job is None:
                continue

            if retcode is None:
                job_result_q.put((job_id, proc))
                status = job.status
            elif retcode == 0:
                status = 'finished'
            else:
                status = 'failed'

            _store_job(SparkJob(
                job_id=job_id,
                name=job.name,
                py_file=job.py_file,
                start_time=job.start_time,
                status=status,
                end_time=time.time(),
            ))
        except Exception as e:
            logging.error('check_work_result error:%s, detail:%s', e, traceback.format_exc())


def _create_db_if_needed():
    conn = sqlite3.connect(spark_db_file)
    try:
        c = conn.cursor()
        c.execute(
            '''
            CREATE TABLE IF NOT EXISTS spark_jobs (
                job_id  text primary key not null,
                name    text not null,
                status  text not null,
                py_file text not null,
                start_time datetime not null,
                end_time datetime
            )
            '''
        )
        conn.commit()
    finally:
        conn.close()


def _jobs_from_store():
    conn = sqlite3.connect(spark_db_file)
    try:
        c = conn.cursor()
        c.execute('''select job_id, name, status, py_file, start_time, end_time
            from spark_jobs order by start_time desc limit 20''')
        rows = c.fetchall()

        ret_jobs = []
        if not rows:
            return ret_jobs

        for row in rows:
            ret_jobs.append(SparkJob(
                job_id=row[0], name=row[1], status=row[2], py_file=row[3], start_time=row[4], end_time=row[5]
            ))
        return ret_jobs
    finally:
        conn.close()


def _job_byid_from_store(job_id):
    conn = sqlite3.connect(spark_db_file)
    try:
        c = conn.cursor()
        c.execute('''select name, status, py_file, start_time, end_time from spark_jobs where job_id=?''', (job_id,))
        row = c.fetchone()
        if row is None:
            return None

        return SparkJob(
            job_id=job_id, name=row[0], status=row[1], py_file=row[2], start_time=row[3], end_time=row[4]
        )
    finally:
        conn.close()


def _store_job(job):
    conn = sqlite3.connect(spark_db_file)
    try:
        c = conn.cursor()
        c.execute('insert or replace into spark_jobs values (?,?,?,?,?,?)', (
            job.job_id, job.name, job.status, job.py_file, job.start_time, job.end_time
        ))

        conn.commit()
    finally:
        conn.close()


def _signal_handler(signum, frame):
    logging.info('Bangu service receives signal: %s. Shutdown now.', signum)
    job_q.close()
    job_result_q.close()
    sys.exit(0)


if __name__ == '__main__':
    _init_logger()

    for sgn in (signal.SIGINT, signal.SIGTERM, signal.SIGQUIT):
        signal.signal(sgn, _signal_handler)

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, default=8998, help='Bangu Service Port')
    args = parser.parse_args()

    spark_db_file = current_work_dir + '/spark_job.db'
    _create_db_if_needed()

    job_files_path = current_work_dir + '/job_files'
    if not os.path.exists(job_files_path):
        os.mkdir(job_files_path)

    n_worker = 8
    job_q = multiprocessing.Queue(n_worker)
    job_result_q = multiprocessing.Queue(n_worker)

    job_threads = [threading.Thread(target=_do_work, name='worker-%d' % i) for i in range(n_worker)]
    job_threads.append(threading.Thread(target=_check_work_result, name='check-result-worker'))

    for t in job_threads:
        t.daemon = True
        t.start()

    app.run(host='0.0.0.0', port=args.port, debug=False)
