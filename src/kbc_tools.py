# coding=utf-8
# Python 3

import base64
import bz2
import csv
import itertools
import json
import pickle
import sys

from collections import deque

import requests
from requests.auth import HTTPBasicAuth

CONNECT_TIMEOUT = 10.01
READ_TIMEOUT = 128

csv.field_size_limit(100 * 2 ** 20)


def slice_stream(iterator, size):
    while True:
        chunk = tuple(itertools.islice(iterator, size))
        if not chunk:
            return
        else:
            yield chunk


def read_csv(input_file):
    safe_input = (line.replace('\0', '') for line in input_file)
    reader = csv.DictReader(safe_input, dialect='kbc')
    while True:
        try:
            yield next(reader)
        except csv.Error as e:
            et = type(e).__name__
            print(
                'could not properly read some row(s) for the input data',
                f'CSV read error, {et}: {e}',
                sep='\n', file=sys.stderr
            )
            sys.stderr.flush()


def make_batch_request(batch, req_obj, *, url, customerId, username, password, session=None, doc_id_key='id', docs_key='documents'):
    auth = HTTPBasicAuth(username, password)
    headers = {
        'Content-Type': 'application/json',
        'X-Customer-ID': f'KBC-{customerId}'
    }
    req = {}
    req.update(req_obj)
    req[docs_key] = list(batch)

    res = json_post(url, auth, headers, req, session=session)
    if not res:
        ids = ' '.join(doc[doc_id_key] for doc in batch)
        print(f'failed to process documents: {ids}', file=sys.stdout)
        print('if the problems persist, please contact our support at support@geneea.com', file=sys.stderr)
        sys.stderr.flush()

    return len(batch) if res else 0


def json_post(url, auth, headers, data, session=None):
    post = session.post if session else requests.post
    try:
        response = post(url, auth=auth, headers=headers, data=json.dumps(data), timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        code = response.status_code
        if code >= 400:
            try:
                msg = response.json()['message']
                print(
                    'Internal error while communicating with the Frida API.',
                    f'HTTP error {code}: {msg}',
                    sep='\n', file=sys.stderr
                )
            except ValueError:
                print(
                    'Internal error while communicating with the Frida API.',
                    f'HTTP error {code}', f'{response.text}',
                    sep='\n', file=sys.stderr
                )
            return None
    except requests.RequestException as e:
        et = type(e).__name__
        print(
            'Internal error while communicating with the Frida API.',
            f'HTTP request exception, {et}: {e}',
            sep='\n', file=sys.stderr
        )
        return None

    return response.json().get('message')


def parallel_map(pool, fn, *iterables, **kwargs):
    argStream = zip(*iterables)
    buffer = deque([pool.submit(fn, *args, **kwargs) for args in list(itertools.islice(argStream, 2 * pool._max_workers))])
    def result_iterator():
        try:
            while buffer:
                future = buffer.popleft()
                yield future.result()
                try:
                    args = next(argStream)
                    buffer.append(pool.submit(fn, *args, **kwargs))
                except StopIteration:
                    pass
        finally:
            for future in buffer:
                future.cancel()
    return result_iterator()


def serialize_data(obj, compress=True):
    bin_data = pickle.dumps(obj)
    if compress:
        bin_data = bz2.compress(bin_data)
    return base64.encodebytes(bin_data).decode('ascii')


def deserialize_data(ser_value, decompress=True):
    bin_data = base64.decodebytes(ser_value.encode('ascii'))
    if decompress:
        bin_data = bz2.decompress(bin_data)
    return pickle.loads(bin_data)
