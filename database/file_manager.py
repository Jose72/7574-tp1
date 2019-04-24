from multiprocessing import Lock
import os
from os import listdir
from os.path import isfile, join
from database.file import File
from database.parsing import numb_to_str_with_zeros, create_new_file_name, last_file

FILE_NAME_START = 'log'
UNDERSCORE = '_'
FILE_EXTENSION = '.csv'

DIGITS_FOR_FILE_NUMBER = 8
DIGITS_FOR_FILE_ID = 8


class FileManager:

    def __init__(self, dir_path, shard_size):
        self.shard_size = shard_size
        self.log_dir = dir_path
        self.lock = Lock()
        self.files = []

        files = get_file_names_in_dir(self.log_dir)

        for f in files:
            self.files.append(File(self.log_dir + f))

    def get_file(self, f_name):
        for f in self.files:
            if f.is_same_file(self.log_dir + f_name):
                return f
        return None

    def get_file_or_append(self, f_name):
        f = self.get_file(f_name)
        if not f:
            f = File(self.log_dir + f_name)
            self.files.append(f)
        return f

    def get_file_names_by_id(self, log_app_id):

        # get the string id
        s_id = numb_to_str_with_zeros(log_app_id, DIGITS_FOR_FILE_ID)

        # search for files with that string
        files = [f for f in listdir(self.log_dir) if isfile(join(self.log_dir, f))]

        # filter the right ones by extension and only take the names
        if len(files):
            files = [f for f in files if FILE_EXTENSION in f]
            files = [f for f in files if s_id in f.split(UNDERSCORE)[1]]

        return files

    def get_file_to_write(self, log_app_id):
        self.lock.acquire()

        try:

            # get the file names with correct id
            files = self.get_file_names_by_id(log_app_id)

            search_f = False

            # check for the last file
            # if there is no last file or it is is full
            # then creating a new one its needed
            last_f = last_file(files)
            if not last_f:
                last_f = create_new_file_name(last_f, log_app_id)
            else:
                f_size = os.path.getsize(self.log_dir + last_f)
                if f_size >= self.shard_size:
                    last_f = create_new_file_name(last_f, log_app_id)
                else:
                    search_f = True

            # if the last file was ok (exist and it is not full)
            # get it and return it
            if search_f:
                return self.get_file_or_append(last_f)

            # if not create the new one and return it
            w_file = File(self.log_dir + last_f)
            self.files.append(w_file)
            return w_file

        finally:
            self.lock.release()

    def get_files_to_read(self, log_app_id):
        self.lock.acquire()

        try:
            # files = self.get_file_names_by_id(log_app_id)
            files = [f for f in self.files if f.is_id(log_app_id)]
            return files

        finally:
            self.lock.release()


def get_file_names_in_dir(dir_path):
    files = [f for f in listdir(dir_path) if isfile(join(dir_path, f))]

    # filter the right ones by extension
    if len(files):
        files = [f for f in files if FILE_EXTENSION in f]

    return files
