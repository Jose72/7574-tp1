from utils.socket import ClientSocket
from multiprocessing import Process
from test.generator import generate_post_request
import json

NEW_LINE = '\n'


def main():
    p = []
    for i in range(0, 10):
        w = PostSender()
        p.append(w)
        w.start()

    for s in p:
        s.join()
    return 0


class PostSender(Process):

    def __init__(self):
        super(PostSender, self).__init__()

    def run(self):

        for i in range(0, 200):
            socket = ClientSocket()
            payload = generate_post_request()
            payload = payload.replace("'", '"')
            payload = json.dumps(json.loads(payload))
            r = 'POST /log HTTP/1.1' + NEW_LINE + \
                'Host: ' + '127.0.0.1:6060' + NEW_LINE + \
                'Content - Length: ' + str(len(payload)) + NEW_LINE + \
                'Content-Type: ' + 'application/json' + NEW_LINE + \
                payload + NEW_LINE
            print(r)
            socket.connect('localhost', 6060)
            socket.send(str(r))
            res = socket.recv(1024)
            socket.close()


if __name__ == '__main__':
    main()
