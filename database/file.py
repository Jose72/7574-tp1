from multiprocessing import Lock
import csv
import json
from database.processing import get_list
import datetime

FILE_NAME_START = 'log'
UNDERSCORE = '_'
FILE_EXTENSION = '.csv'

DIGITS_FOR_FILE_NUMBER = 8
DIGITS_FOR_FILE_ID = 8


class File:

    def __init__(self, file_path):

        self.file_path = file_path
        self.lock = Lock()

    def read_logs(self, fieldnames, q_tags, q_date_from, q_date_to, q_pattern):

        self.lock.acquire()

        try:
            with open(self.file_path, 'r') as cf:

                reader = csv.DictReader(cf, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL,
                                        fieldnames=fieldnames)

                result = []

                for e in reader:

                    e = json.loads(json.dumps(e))

                    cond_tags = True
                    cond_date = True

                    l_tags = get_list(e['logTags'])
                    l_date = e['timestamp']
                    l_message = e['message']

                    # Check if the tags are correct
                    if len(q_tags):
                        cond_tags = set(q_tags).issubset(set(l_tags))

                    # Check correct date
                    if q_date_from:
                        cond_date = (datetime.datetime.strptime(q_date_from, '%b %d %Y %I:%M%p') <=
                                         datetime.datetime.strptime(l_date, '%b %d %Y %I:%M%p'))

                    if q_date_to:
                        cond_date = (datetime.datetime.strptime(q_date_to, '%a, %d %b %Y %H:%M:%S GMT') >=
                                         datetime.datetime.strptime(l_date, '%a, %d %b %Y %H:%M:%S GMT'))
                    # 'Sun, 07 Apr 2019 22:07:55 GMT'

                    cond_pattern = (q_pattern in l_message)

                    print("tags: " + str(cond_tags) + " date: " + str(cond_date) + "pattern: " + str(cond_pattern))

                    if cond_tags & cond_date & cond_pattern:
                        result.append(e)
            cf.close()
            return result

        finally:
            self.lock.release()

    def write_log(self, log, fieldnames):

        self.lock.acquire()

        try:
            with open(self.file_path, 'a+') as cf:
                writer = csv.DictWriter(cf, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL,
                                        fieldnames=fieldnames)
                writer.writerow(log)
                cf.close()

        finally:
            self.lock.release()

    def is_same_file(self, f_path):
        return self.file_path == f_path
