from utils.socket import ClientSocket
from multiprocessing import Process
from test.generator import generate_post_request, generate_get_request
import json

NEW_LINE = '\n'
GETS_TO_SEND_PER_PROCESS = 50
POSTS_TO_SEND_PER_PROCESS = 500
GET_PROCESSES = 4
POST_PROCESSES = 4


def main():
    p = []
    for i in range(0, POST_PROCESSES):
        wp = PostSender()
        p.append(wp)
        wp.start()

    for i in range(0, GET_PROCESSES):
        wg = GetSender()
        p.append(wg)
        wg.start()

    for s in p:
        s.join()
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
            #print(r)
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
            #print(r)
            socket.connect('localhost', 6070)
            socket.send(str(r))
            res = socket.recv(1024)
            #print(res)
            while res:
                res = socket.recv(1024)
            socket.close()


if __name__ == '__main__':
    main()
