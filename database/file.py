from multiprocessing import Lock
import csv
import json
from database.parsing import get_list, numb_to_str_with_zeros
import datetime as dt

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

            result = []

            with open(self.file_path, 'r') as cf:

                reader = csv.DictReader(cf, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL,
                                        fieldnames=fieldnames)

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
                        cond_date = cond_date & (dt.datetime.strptime(q_date_from, '%Y/%m/%d %H:%M:%S.%f') <=
                                     dt.datetime.strptime(l_date, '%Y/%m/%d %H:%M:%S.%f'))

                    if q_date_to:
                        cond_date = cond_date & (dt.datetime.strptime(q_date_to, '%Y/%m/%d %H:%M:%S.%f') >=
                                     dt.datetime.strptime(l_date, '%Y/%m/%d %H:%M:%S.%f'))

                    cond_pattern = (q_pattern in l_message)

                    #print("tags: " + str(cond_tags) + " date: " + str(cond_date) + "pattern: " + str(cond_pattern))

                    if cond_tags & cond_date & cond_pattern:
                        result.append(e)
                        #print("partial: " + str(result))

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

    def is_id(self, f_id):
        x = (self.file_path.split('/'))[-1]
        x = (x.split(UNDERSCORE))[1]
        return (numb_to_str_with_zeros(f_id, DIGITS_FOR_FILE_ID)) == x
