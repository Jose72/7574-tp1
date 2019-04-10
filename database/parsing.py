# Given an integer and an amount of digits, creates a string
# with the integer an pads the rest with 0's in the beginning
# returns full 0's if the length on the number surpasses the amount of digits

DIGITS_FOR_FILE_NUMBER = 8
DIGITS_FOR_FILE_ID = 8
FILE_NAME_START = 'log'
UNDERSCORE = '_'
FILE_EXTENSION = '.csv'


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


def create_new_file_name(last_f_name, app_id):
    if last_f_name:
        s = str.split(last_f_name, UNDERSCORE)
        new_f_name = s[0] + UNDERSCORE + s[1] + UNDERSCORE + numb_to_str_with_zeros(
            (int(s[2].replace(FILE_EXTENSION, '')) + 1), DIGITS_FOR_FILE_NUMBER)
    else:
        new_f_name = FILE_NAME_START + UNDERSCORE + numb_to_str_with_zeros(int(app_id), DIGITS_FOR_FILE_ID) + \
                     UNDERSCORE + numb_to_str_with_zeros(1, DIGITS_FOR_FILE_NUMBER)
    return new_f_name + FILE_EXTENSION
