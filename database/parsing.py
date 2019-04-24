import datetime as dt

DIGITS_FOR_FILE_NUMBER = 8
DIGITS_FOR_FILE_ID = 8
FILE_NAME_START = 'log'
UNDERSCORE = '_'
FILE_EXTENSION = '.csv'
ID_POS_IN_FILE_NAME = 1
NUM_FILE_POS_IN_FILE_NAME = 3
DATE_POS_IN_FILE_NAME = 2
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S.%f'


def numb_to_str_with_zeros(num, digits):
    n = str(num)
    return n.zfill(digits)


def last_file(files):
    if not len(files):
        return ''
    l_file = files[0]
    for f in files:
        if int(l_file.split(UNDERSCORE)[NUM_FILE_POS_IN_FILE_NAME].replace(FILE_EXTENSION, '')) < \
                int(f.split(UNDERSCORE)[NUM_FILE_POS_IN_FILE_NAME].replace(FILE_EXTENSION, '')):
            l_file = f
    return l_file


def get_list(s):
    s = s.replace("\'", '')
    s = s.replace("[", '')
    s = s.replace("]", '')
    s = s.split(',')
    return [x.strip() for x in s]


def create_new_file_name(last_f_name, app_id, log_timestamp):
    if last_f_name:
        s = str.split(last_f_name, UNDERSCORE)
        new_f_name = s[0] + UNDERSCORE + s[1] + UNDERSCORE + s[2] + UNDERSCORE + \
            numb_to_str_with_zeros((int(s[3].replace(FILE_EXTENSION, '')) + 1), DIGITS_FOR_FILE_NUMBER)
    else:
        date = dt.datetime.strptime(log_timestamp, LOG_DATE_FORMAT).strftime("%Y-%m-%d")
        new_f_name = FILE_NAME_START + UNDERSCORE +\
            numb_to_str_with_zeros(int(app_id), DIGITS_FOR_FILE_ID) + UNDERSCORE +\
            date + UNDERSCORE +\
            numb_to_str_with_zeros(1, DIGITS_FOR_FILE_NUMBER)
    return new_f_name + FILE_EXTENSION
