from multiprocessing import Process
from utils.parser import HttpParser
from threading import Thread
import signal
import socket


class ResponseHandler(Thread):
    def __init__(self, i, code, pending_req_queue):
        super(ResponseHandler, self).__init__()

        self.worker_id = i
        self.code = code
        self.queue = pending_req_queue
        self.end = False

    def run(self):

        print(str(self.code) + " Response Handler: " + str(self.worker_id) + " - Started")

        while not self.end:

            p = self.queue.get()

            if p == "end":
                print(str(self.code) + " Response Handler: " + str(self.worker_id) + " - Finished")
                return

            c_sock = p[0]
            db_sock = p[1]

            try:
                r_size = db_sock.recv_f(8)
                result = db_sock.recv_f(int(r_size))

                result = HttpParser.generate_response('200 OK', result)
                print(str(self.code) + " Response Handler: " + str(self.worker_id) + ' - Data: \n' + str(result))
                c_sock.send(result)

            finally:
                if db_sock:
                    db_sock.shutdown(socket.SHUT_RDWR)
                    db_sock.close()
                if c_sock:
                    c_sock.shutdown(socket.SHUT_RDWR)
                    c_sock.close()