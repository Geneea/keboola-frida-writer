# coding=utf-8
# Python 3

import itertools
import json
import os
import re
import sys

import requests

from concurrent.futures import ThreadPoolExecutor

from keboola import docker

from kbc_tools import read_csv, slice_stream, make_batch_request, parallel_map, deserialize_data

FRIDA_URL = 'https://frida.geneea.com/services/franz'
DOC_BATCH_SIZE = 5
THREAD_COUNT = 1

DATASET_RE = re.compile(r'^[0-9a-zA-Z_\-]+$')

class Params:

    def __init__(self, config):
        self.config = config

        self.customer_id = os.getenv('KBC_PROJECTID')

        self.source_tab_path = self.get_source_tab_path()

        params = config.get_parameters()
        columns = params.get('columns', {})
        if not isinstance(columns, dict):
            columns = {}

        self.dataset = params.get('dataset')
        self.username = params.get('username')
        self.password = params.get('password')
        self.id_col = columns.get('id')
        self.data_col = columns.get('binaryData')
        self.meta_cols = columns.get('metadata', [])

        advanced_params = self.get_advanced_params()
        self.doc_batch_size = int(advanced_params.get('doc_batch_size', DOC_BATCH_SIZE))
        self.thread_count = int(advanced_params.get('thread_count', THREAD_COUNT))

        self.validate()

    def get_source_tab_path(self):
        in_tabs = self.config.get_input_tables()
        return in_tabs[0]['full_path'] if len(in_tabs) == 1 else None

    def get_advanced_params(self):
        advanced_params = self.config.get_parameters().get('advanced', {})
        return advanced_params if isinstance(advanced_params, dict) else {}

    def validate(self):
        if not self.config.get_parameters():
            raise ValueError('missing configuration parameters in "config.json"')
        if self.customer_id is None:
            raise ValueError('the "KBC_PROJECTID" environment variable needs to be set')
        if self.source_tab_path is None:
            raise ValueError('exactly one INPUT table mapping needs to be specified')
        if not self.dataset or not DATASET_RE.match(self.dataset):
            raise ValueError('invalid "dataset" parameter')
        if not self.username or not self.password:
            raise ValueError('the "username" and "password" are required parameters')
        if not self.id_col or not self.data_col:
            raise ValueError('the "columns.id" and "columns.binaryData" are required parameters')
        if self.meta_cols and not isinstance(self.meta_cols, list):
            raise ValueError('invalid "columns.metadata" parameter, it needs to be an array of column names')
        if self.thread_count > 8:
            raise ValueError('the "thread_count" parameter can not be greater than 8')

    def get_usage_path(self):
        return os.path.normpath(os.path.join(
            self.config.get_data_dir(), 'out', 'usage.json'
        ))

    @staticmethod
    def init(data_dir=''):
        return Params(docker.Config(data_dir))


class WriterApp:

    def __init__(self, *, data_dir=''):
        self.params = Params.init(data_dir)
        self.validate_input()

    def validate_input(self):
        with open(self.params.source_tab_path, 'r', encoding='utf-8') as in_tab:
            try:
                row = next(read_csv(in_tab))
            except StopIteration:
                print('WARN: could not read any data from the source table')
                sys.stdout.flush()
                return
            all_cols = [self.params.id_col, self.params.data_col] + self.params.meta_cols
            for col in all_cols:
                if col not in row:
                    raise ValueError(f'the source table does not contain column "{col}"')

    def run(self):
        print(f'starting export to Frida dataset "{self.params.dataset}"')
        sys.stdout.flush()
        doc_count = 0

        with open(self.params.source_tab_path, 'r', encoding='utf-8') as in_tab:
            for count in self.inject(read_csv(in_tab)):
                doc_count += count
                if doc_count % (100 * self.params.doc_batch_size) == 0:
                    self.write_usage(doc_count=doc_count)
                    print(f'successfully exported {doc_count} documents')
                    sys.stdout.flush()

        self.write_usage(doc_count=doc_count)
        print(f'the export has finished successfully, {doc_count} documents were exported')
        sys.stdout.flush()

    def inject(self, row_stream):
        url = f'{FRIDA_URL}/datasets/{self.params.dataset}/documents/update'
        req = self.get_request()
        batch_stream = self.doc_batch_stream(row_stream)

        with requests.Session() as session:
            with ThreadPoolExecutor(max_workers=self.params.thread_count) as executor:
                for res in parallel_map(
                        executor, make_batch_request,
                        batch_stream, itertools.repeat(req),
                        url=url, customerId=self.params.customer_id,
                        username=self.params.username, password=self.params.password,
                        session=session
                ):
                    yield res

    def get_request(self):
        req = {}
        if self.params.meta_cols:
            req['metadata'] = [f'f_{meta_col}' for meta_col in self.params.meta_cols]
        return req

    def doc_batch_stream(self, row_stream):
        for rows in slice_stream(row_stream, self.params.doc_batch_size):
            yield list(map(self.row_to_doc, rows))

    def row_to_doc(self, row):
        analysis = deserialize_data(row[self.params.data_col])
        doc = {
            'id': row[self.params.id_col],
            'title': analysis.get('title', ''),
            'lead': analysis.get('lead', ''),
            'text': analysis.get('text', ''),
            'titleLemmas': [],
            'leadLemmas': [],
            'textLemmas': [],
            'language': {'value': analysis['language']},
            'entities': [],
            'hashtags': []
        }

        if 'sentiment' in analysis:
            doc['sentiment'] = {
                'value': analysis['sentiment']['value'],
                'label': analysis['sentiment']['label']
            }

        if 'sentences' in analysis:
            for sent in analysis['sentences']:
                toks = [{
                    'val': t.get('lemma'),
                    'idx': t['idx'],
                    'off': t['off'],
                    'len': t['len']
                } for t in sent.get('tokens', [])]
                sgm = sent.get('segment', 'text')
                doc[f'{sgm}Lemmas'].append(toks)

        if 'entities' in analysis:
            for ent in analysis['entities']:
                inst = [{
                    'segment': m['segment'],
                    'offset': m['offset'],
                    'tokenIndices': m['tokenIndices']
                } for m in ent.get('mentions', [])]
                if ent['type'] in {'topic', 'tag'}:
                    doc['hashtags'].append({
                        'value': ent['text'],
                        'type': ent['type'],
                        'uid': ent.get('uid'),
                        'instances': inst
                    })
                else:
                    doc['entities'].append({
                        'standardForm': ent['text'],
                        'type': ent['type'],
                        'uid': ent.get('uid'),
                        'instances': inst
                    })

        for meta_col in self.params.meta_cols:
            doc[f'f_{meta_col}'] = row[meta_col]

        return doc

    def write_usage(self, *, doc_count):
        usage_path = self.params.get_usage_path()
        with open(usage_path, 'w', encoding='utf-8') as usage_file:
            json.dump([
                {'metric': 'documents', 'value': doc_count},
                {'metric': 'processing_threads', 'value': self.params.thread_count}
            ], usage_file, indent=4)
