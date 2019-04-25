
import random
import string


timestamps = ["2017-01-10 16:22:42.158852", "2017-01-13 11:53:42.158852", "2017-02-20 16:53:42.158852",
              "2017-02-03 16:53:42.158852", "2017-03-12 18:53:42.158852", "2017-04-22 16:53:42.158852",
              "2018-01-04 17:53:42.158852", "2018-01-23 16:53:42.158852", "2018-02-20 16:53:42.158852",
              "2018-04-05 16:22:42.158852", "2018-09-23 16:53:42.158852", "2018-11-20 16:53:42.158852",
              "2018-12-05 23:53:42.158852", "2018-12-23 16:53:42.158852", "2018-12-30 16:53:42.158852",
              "2019-02-06 16:53:42.158852", "2019-03-06 16:53:42.158852", "2019-04-05 12:53:42.158852",
              "2019-04-10 16:53:42.158852"
              ]

from_to = [["", "2017-02-03 16:53:42.158852"],
           ["2017-03-01 16:22:42.158852", "2017-06-03 16:53:42.158852"],
           ["2018-01-01 16:22:42.158852", "2018-06-01 16:53:42.158852"],
           ["2018-06-01 16:22:42.158852", "2018-12-30 16:53:42.158852"],
           ["2019-01-01 16:22:42.158852", ""]
           ]

timestamps_up = []

tags = ["Tag_1", "Tag_2", "Tag_3", "Tag_4", "Tag_5",
        "Tag_6",  "Tag_7", "Tag_8", "Tag_9", "Tag_10",
        ]

msgs = ["All good", "Super good", "Good", "Super bad",
        "Bad", "Really bad", "Horrible", "Not done",
        "Finished", "Trash", "Impossible",
        "In process", "Interrupted", "Wrong file",
        "Could not find", "Wrong number",
        "Great", "Not zero",
        "Not",
        "Value not found",
        "Divided by zero"
        ]

patterns = ["good", "bad", "", "", "", "wrong", "zero", "", "number", "find"]

def generate_post_request():
    _id = generate_id()
    tags = generate_tags(10, 9)
    timestamp = generate_timestamp()
    msg = generate_message()
    return '{"AppId":"' + str(_id) + '",' + \
            '"logTags":' + str(tags) + ',' + \
            '"timestamp":"' + str(timestamp) + '",' + \
            '"message":"' + str(msg) + '"' + \
            '}'

def generate_get_request():
    _id = generate_id()
    tags = generate_tags(10, 8)
    ft = generate_from_to()
    _from = ft[0]
    _to = ft[1]
    msg = generate_pattern()
    return '{"AppId":"' + str(_id) + '",' + \
            '"logTags":' + str(tags) + ',' + \
            '"From":"' + str(_from) + '",' + \
            '"To":"' + str(_to) + '", ' + \
            '"pattern":"' + str(msg) + '"' + \
            '}'

def generate_pattern():
    i = random.randint(0, len(patterns))
    return patterns[i - 1]

def generate_from_to():
    i = random.randint(0, len(from_to))
    return from_to[i - 1]

def generate_tags(max, l_limit):
    tags_s = []
    while len(tags_s) < 1:
        for i in range(1, len(tags)):
            if random.randint(0, max) >= l_limit:
                tags_s.append(tags[i-1])
    return tags_s


def generate_timestamp():
    i = random.randint(0, len(timestamps))
    return timestamps[i-1]


def generate_id():
    return random.randint(0, 10)


def generate_message():
    i = random.randint(0, len(msgs))
    return msgs[i-1]


