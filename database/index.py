import fcntl
import csv
from database.parsing import get_list
import os

INDEX_EXTENSION = 'csv'


class Indexer:

    @staticmethod
    def index_file(file_path, fieldnames):
        index_file_path = get_index_file_path(file_path)
        tag_list_index = TagListIndex()
        with open(file_path, 'r+') as l_file:
            fcntl.flock(l_file, fcntl.LOCK_SH)
            reader = csv.DictReader(l_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL,
                                    fieldnames=fieldnames)

            c = 0
            for e in reader:
                l_tags = get_list(e['logTags'])
                for tag in l_tags:
                    tag_list_index.append(tag, c)
                c += 1

            fcntl.flock(l_file, fcntl.LOCK_UN)
            l_file.close()

        if not tag_list_index.empty():
            with open(index_file_path, 'w') as i_file:

                fcntl.flock(i_file, fcntl.LOCK_EX)

                i_fieldnames = ['tag', 'indexes']
                writer = csv.DictWriter(i_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL,
                                        fieldnames=i_fieldnames)

                for i in range(0, tag_list_index.size()):
                    # print(tag_list_index.get(i))
                    writer.writerow(tag_list_index.get(i))

                i_file.flush()

                fcntl.flock(i_file, fcntl.LOCK_UN)
                i_file.close()

        return index_file_path

    @staticmethod
    def look_for_index_file(file_path):
        index_file_path = get_index_file_path(file_path)
        if not os.path.isfile(index_file_path):
            return None
        return index_file_path


class TagListIndex:

    def __init__(self):
        self.tag_names = []
        self.tag_index_list = []

    def append(self, tag, index):
        if tag:
            try:
                idx = self.tag_names.index(tag)
                self.tag_index_list[idx].append(index)
            except ValueError:
                self.tag_names.append(tag)
                self.tag_index_list.append([index])

    def empty(self):
        return len(self.tag_names) == 0

    def size(self):
        return len(self.tag_names)

    def get(self, idx):
        return {'tag': self.tag_names[idx], 'indexes': self.tag_index_list[idx]}


def get_index_file_path(file_path):
    (dir_path, filename) = file_path.rsplit('/', 1)
    filename = filename.rsplit('.', 1)[0]
    return dir_path + '/' + 'index' + '_' + filename + '.' + INDEX_EXTENSION
