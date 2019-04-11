import json
import datetime
import os
from os import listdir
from os.path import isfile, join
import csv
import datetime as dt

DIGITS_FOR_FILE_NUMBER = 8
DIGITS_FOR_FILE_ID = 8
FILE_NAME_START = 'log'
UNDERSCORE = '_'
FILE_EXTENSION = '.csv'


class DBProcessRequest:

    def __init__(self, dir_path, shard_size, file_manager):

        # if the directory doe not exist then create it
        if not os.path.exists(dir_path):
            os.mkdir(dir_path, 0o755)

        self.log_dir = dir_path
        self.shard_size = shard_size
        self.file_manager = file_manager

    def process_get(self, queries):

        result = []

        json_queries = json.loads(queries)

        fieldnames = ['logTags', 'message', 'timestamp']

        q_app_id = json_queries['AppId']
        q_tags = json_queries['logTags']
        q_date_from = json_queries['From']
        q_date_to = json_queries['To']
        q_pattern = json_queries['pattern']

        files = self.file_manager.get_files_to_read(q_app_id)

        for f in files:
            result += f.read_logs(fieldnames, q_tags, q_date_from, q_date_to, q_pattern)

        # formating result
        result = json.dumps(result)

        return result

    def process_post(self, logs):

        json_logs = (json.loads(logs))['logs']

        fieldnames = ['logTags', 'message', 'timestamp']

        for e in json_logs:
            log_app_id = e['AppId']
            del e['AppId']
            w_file = self.file_manager.get_file_to_write(log_app_id)
            w_file.write_log(e, fieldnames)

        return '200 OK'

    def process(self, code, request):
        if code == 'GET':
            return self.process_get(request)
        if code == 'POST':
            return self.process_post(request)
        return ''


