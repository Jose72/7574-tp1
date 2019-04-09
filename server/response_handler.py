from multiprocessing import Process
from utils.parser import HttpParser
import signal


class ResponseHandler(Process):

    def __init__(self, i, code, queue):
        super(ResponseHandler, self).__init__()

        self.worker_id = i
        self.code = code
        self.queue = queue
        self.end = False

    def run(self):

        signal.signal(signal.SIGINT, signal.SIG_IGN)

        print(str(self.code) + " Response Handler: " + str(self.worker_id) + " - Started")

        while not self.end:

            p = self.queue.get()

            if p == "end":
                print(str(self.code) + " Response Handler: " + str(self.worker_id) + " - Finished")
                return

            c_sock = p[0]
            db_sock = p[1]

            r_size = db_sock.recv_f(4)
            result = db_sock.recv_f(int(r_size))

            result = HttpParser.generate_response('200 OK', result)
            print(str(self.code) + " Response Handler: " + str(self.worker_id) + ' - Data: \n' + str(result))
            c_sock.send(result)

            db_sock.close()
            c_sock.close()
