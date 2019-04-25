from utils.socket import ClientSocket
from multiprocessing import Process
from test.generator import generate_post_request, generate_get_request
import json
import datetime as dt

NEW_LINE = '\n'
GETS_TO_SEND_PER_PROCESS = 100
POSTS_TO_SEND_PER_PROCESS = 300
GET_PROCESSES = 0
POST_PROCESSES = 5


def main():

    p = []
    for i in range(0, POST_PROCESSES):
        wp = PostSender()
        p.append(wp)
        wp.start()

    t1 = dt.datetime.now()
    print(t1)

    for i in range(0, GET_PROCESSES):
        wg = GetSender()
        p.append(wg)
        wg.start()

    for s in p:
        s.join()

    t2 = dt.datetime.now()
    print(t2)
    return 0


class PostSender(Process):

    def __init__(self):
        super(PostSender, self).__init__()

    def run(self):

        for i in range(0, POSTS_TO_SEND_PER_PROCESS):
            socket = ClientSocket()
            payload = generate_post_request()
            payload = payload.replace("'", '"')
            payload = json.dumps(json.loads(payload))
            r = 'POST /log HTTP/1.1' + NEW_LINE + \
                'Host: ' + '127.0.0.1:6060' + NEW_LINE + \
                'Content - Length: ' + str(len(payload)) + NEW_LINE + \
                'Content-Type: ' + 'application/json' + NEW_LINE + \
                payload + NEW_LINE
            # print(r)
            socket.connect('localhost', 6060)
            socket.send(str(r))
            res = socket.recv(1024)
            socket.close()


class GetSender(Process):
    def __init__(self):
        super(GetSender, self).__init__()

    def run(self):

        for i in range(0, GETS_TO_SEND_PER_PROCESS):
            socket = ClientSocket()
            payload = generate_get_request()
            payload = payload.replace("'", '"')
            payload = json.dumps(json.loads(payload))
            r = 'GET /log HTTP/1.1' + NEW_LINE + \
                'Host: ' + '127.0.0.1:6070' + NEW_LINE + \
                'Content - Length: ' + str(len(payload)) + NEW_LINE + \
                'Content-Type: ' + 'application/json' + NEW_LINE + \
                payload + NEW_LINE
            # print(r)
            socket.connect('localhost', 6070)
            socket.send(str(r))
            res = socket.recv(1024)
            print(res)
            while res:
                res = socket.recv(1024)
            socket.close()


if __name__ == '__main__':
    main()
