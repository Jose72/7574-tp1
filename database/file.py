import csv
import json
from database.parsing import get_list, numb_to_str_with_zeros
import datetime as dt
import os
import fcntl
from database.index import Indexer

from database.constants import LOG_DATE_FORMAT
from database.constants import DIGITS_FOR_FILE_ID
from database.constants import UNDERSCORE
from database.constants import NAME_DATE_FORMAT
from database.constants import DATE_POS_IN_FILE_NAME


class File:

    def __init__(self, file_path):

        self.file_path = file_path
        self.index_file_path = Indexer.look_for_index_file(self.file_path)

        # if file does not exists, create it
        if not os.path.isfile(self.file_path):
            f = open(self.file_path, 'a+')
            f.close()

    def read_logs(self, fieldnames, q_tags, q_date_from, q_date_to, q_pattern):

        result = []

        tag_index_list = []

        index_file_exists = bool(self.index_file_path)

        filter_by_tags = (len(q_tags) > 0)

        # if there is an index file and i have to filter by tags
        # get the indexes
        if index_file_exists & filter_by_tags:
            tag_index_list = get_tag_indexes(self.index_file_path, q_tags)
            # if my index list is empty just return
            if not tag_index_list:
                return result

        with open(self.file_path, 'r') as cf:

            fcntl.flock(cf, fcntl.LOCK_SH)

            reader = csv.DictReader(cf, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL,
                                    fieldnames=fieldnames)

            n_line = 0
            for chunk in gen_row(reader):
                n_line += 1
                e = json.loads(json.dumps(chunk))

                cond_tags = True
                cond_date = True

                l_tags = get_list(e['logTags'])
                l_date = e['timestamp']
                l_message = e['message']

                # If i have to filter by tags
                if filter_by_tags:
                    # check if i have an index file
                    if index_file_exists:
                        # if so, check for correct index
                        if str(n_line - 1) not in tag_index_list:
                            continue
                    # if not do normal tag check
                    else:
                        cond_tags = set(q_tags).issubset(set(l_tags))

                # Check correct date
                if q_date_from:
                    cond_date = cond_date & (dt.datetime.strptime(q_date_from, LOG_DATE_FORMAT) <=
                                             dt.datetime.strptime(l_date, LOG_DATE_FORMAT))

                if q_date_to:
                    cond_date = cond_date & (dt.datetime.strptime(q_date_to, LOG_DATE_FORMAT) >=
                                             dt.datetime.strptime(l_date, LOG_DATE_FORMAT))

                # Check correct pattern
                cond_pattern = (q_pattern in l_message)

                # print("tags: " + str(cond_tags) + " date: " + str(cond_date) + "pattern: " + str(cond_pattern))

                if cond_tags & cond_date & cond_pattern:
                    result.append(e)
                    # print("partial: " + str(result))

            fcntl.flock(cf, fcntl.LOCK_UN)

        cf.close()
        return result

    def write_log(self, log, fieldnames):

        with open(self.file_path, 'a+') as cf:

            fcntl.flock(cf, fcntl.LOCK_SH)

            writer = csv.DictWriter(cf, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL,
                                    fieldnames=fieldnames)
            writer.writerow(log)

            cf.flush()

            fcntl.flock(cf, fcntl.LOCK_UN)

            cf.close()

    def create_index_file(self, fieldnames):
        self.index_file_path = Indexer.index_file(self.file_path, fieldnames)

    def is_same_file(self, f_path):
        return self.file_path == f_path

    def is_id(self, f_id):
        x = (self.file_path.split('/'))[-1]
        x = (x.split(UNDERSCORE))[1]
        return (numb_to_str_with_zeros(f_id, DIGITS_FOR_FILE_ID)) == x

    def is_date(self, d_from, d_to):
        x = (self.file_path.split('/'))[-1]
        x = (x.split(UNDERSCORE))[DATE_POS_IN_FILE_NAME]
        date = dt.datetime.strptime(x, NAME_DATE_FORMAT)
        cond_from = True
        cond_to = True
        if d_from:
            cond_from = date >= dt.datetime.strptime(dt.datetime.strptime(
                d_from, LOG_DATE_FORMAT).strftime(NAME_DATE_FORMAT), NAME_DATE_FORMAT)
        if d_to:
            cond_to = date <= dt.datetime.strptime(dt.datetime.strptime(
                d_to, LOG_DATE_FORMAT).strftime(NAME_DATE_FORMAT), NAME_DATE_FORMAT)
        return cond_from & cond_to

    def get_file_name(self):
        return self.file_path.split('/')[-1]


def gen_row(reader):
    for row in reader:
        yield row


def get_tag_indexes(index_file_path, q_tags):
    index_list = []
    with open(index_file_path, 'r+') as i_file:
        fcntl.flock(i_file, fcntl.LOCK_SH)

        reader = csv.DictReader(i_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL,
                                fieldnames=['tag', 'indexes'])

        for e in gen_row(reader):
            i_tag = e['tag']
            i_indexes = get_list(e['indexes'])
            if i_tag in q_tags:
                if index_list:
                    index_list = list(set(index_list) & set(i_indexes))
                else:
                    index_list = i_indexes

        fcntl.flock(i_file, fcntl.LOCK_UN)
        i_file.close()

    return index_list
