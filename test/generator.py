
import random
import string


timestamps = ["2017/04/10 16:22:42.158852",
              "2017/04/03 16:53:42.158852",
              "2018/01/04 17:53:42.158852",
              "2018/04/05 16:22:42.158852",
              "2018/12/05 23:53:42.158852",
              "2019/02/06 16:53:42.158852",
              "2019/023/06 106:53:42.158852",
              "2019/04/05 12:53:42.158852",
              "2019/04/10 16:53:42.158852"
              ]

tags = ["Tag_1",
        "Tag_2",
        "Tag_3",
        "Tag_4",
        "Tag_5"]

msgs = ["All good",
        "Bad",
        "Finished",
        "In process",
        "Could not find",
        "Great",
        "Not",
        "Value not found",
        "Divided by Zero"
        ]

def generate_post_request():
    _id = generate_id()
    tags = generate_tags()
    timestamp = generate_timestamp()
    msg = generate_message()
    return '{"AppId":"' + str(_id) + '",' + \
            '"logTags":' + str(tags) + ',' + \
            '"timestamp":"' + str(timestamp) + '",' + \
            '"message":"' + str(msg) + '"' + \
            '}'


def generate_tags():
    tags_s = []
    for i in range(1, 6):
        if random.randint(0, 5) >= 4:
            tags_s.append(tags[i-1])
    return tags_s


def generate_timestamp():
    i = random.randint(0, len(timestamps))
    return timestamps[i-1]


def generate_id():
    return random.randint(0, 5)


def generate_message():
    i = random.randint(0, len(msgs))
    return msgs[i-1]


