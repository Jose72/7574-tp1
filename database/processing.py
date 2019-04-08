import json
import datetime
import os
from os import listdir
from os.path import isfile, join
import csv

DIGITS_FOR_FILE_NUMBER = 8
DIGITS_FOR_FILE_ID = 8
FILE_NAME_START = 'log'
UNDERSCORE = '_'
FILE_EXTENSION = '.csv'


class DBProcessRequest:

    def __init__(self, dir_path, shard_size, lock):

        # if the directory doe not exist then create it
        if not os.path.exists(dir_path):
            os.mkdir(dir_path, 0o755)

        self.log_dir = dir_path
        self.shard_size = shard_size
        self.lock = lock

    def process_get(self, queries):

        result = []

        # The queries are received as a json string
        json_queries = json.loads(queries)
        #json_queries = queries

        fieldnames = ['logTags', 'message', 'timestamp']

        q_appid = json_queries['AppId']
        q_tags = json_queries['logTags']
        q_date_from = json_queries['From']
        q_date_to = json_queries['To']
        q_pattern = json_queries['pattern']

        # Get a list of files with the corresponding app id
        # If there was no AppId query then take all the files
        s_id = numb_to_str_with_zeros(q_appid, DIGITS_FOR_FILE_ID)
        files = [f for f in listdir(self.log_dir) if isfile(join(self.log_dir, f))]
        if q_appid:
            files = [f for f in files if s_id in f.split(UNDERSCORE)[1]]

        # For each file
        for f in files:

            f_size = os.path.getsize(self.log_dir + f)

            with open(self.log_dir + f, 'r') as cf:
                reader = csv.DictReader(cf, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL,
                                        fieldnames=fieldnames)

                # If the file can be writen locking its needed
                can_write_file = f_size >= self.shard_size

                if can_write_file:
                    self.lock.acquire()

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

                if can_write_file:
                    self.lock.release()

        print(result)
        return result

    def process_post(self, logs):

        # print(str(type(logs)) + " - " + str(logs))

        json_logs = (json.loads(logs))['logs']

        fieldnames = ['logTags', 'message', 'timestamp']

        for e in json_logs:
            l_appid = e['AppId']
            del e['AppId']
            s_id = numb_to_str_with_zeros(l_appid, DIGITS_FOR_FILE_ID)

            # Get a list of files with the corresponding app id
            files = [f for f in listdir(self.log_dir) if isfile(join(self.log_dir, f))]

            if len(files):
                files = [f for f in files if '.csv' in f]
                files = [f for f in files if s_id in f.split(UNDERSCORE)[1]]

            #print("files: " + str(files))

            # look for the last file
            last_f = last_file(files)

            # If no file was found create a new one,
            # else take the size of the file
            f_size = 0
            if not last_f:
                last_f = create_new_file_name(last_f, l_appid)
            else:
                f_size = os.path.getsize(self.log_dir + last_f)

            #print(last_f + " - fsize: " + str(f_size))

            # Check if there is enough space in the file,
            # if not create new file
            if f_size >= self.shard_size:
                last_f = create_new_file_name(last_f, l_appid)
                print("new file: " + last_f)

            # Open file and write data
            # lock the writing
            with open(self.log_dir + last_f, 'a+') as cf:
                print("file opened: " + self.log_dir + last_f)
                writer = csv.DictWriter(cf, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL,
                                        fieldnames=fieldnames)
                self.lock.acquire()
                writer.writerow(e)
                self.lock.release()
                cf.close()

        return '200 OK'

    def process(self, code, request):
        if code == 'GET':
            return self.process_get(request)
        if code == 'POST':
            return self.process_post(request)
        return ''


def create_new_file_name(last_f_name, app_id):
    if last_f_name:
        s = str.split(last_f_name, UNDERSCORE)
        new_f_name = s[0] + UNDERSCORE + s[1] + UNDERSCORE + numb_to_str_with_zeros(
            (int(s[2].replace(FILE_EXTENSION, '')) + 1), DIGITS_FOR_FILE_NUMBER)
    else:
        new_f_name = FILE_NAME_START + UNDERSCORE + numb_to_str_with_zeros(int(app_id), DIGITS_FOR_FILE_ID) + \
                     UNDERSCORE + numb_to_str_with_zeros(1, DIGITS_FOR_FILE_NUMBER)
    return new_f_name + FILE_EXTENSION


# Given an integer and an amount of digits, creates a string
# with the integer an pads the rest with 0's in the beginning
# returns full 0's if the length on the number surpasses the amount of digits
def numb_to_str_with_zeros(num, digits):
    n = str(num)
    z = 0
    if len(n) < digits:
        z = digits - len(n)
    return n.zfill(z)


def last_file(files):
    if not len(files):
        return ''
    l_file = files[0]
    for f in files:
        if int(l_file.split(UNDERSCORE)[2].replace(FILE_EXTENSION, '')) < \
                int(f.split(UNDERSCORE)[2].replace(FILE_EXTENSION, '')):
            l_file = f
    return l_file


def get_list(s):
    s = s.replace("\'", '')
    s = s.replace("[", '')
    s = s.replace("]", '')
    s = s.split(',')
    return [x.strip() for x in s]
