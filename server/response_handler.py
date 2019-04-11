from utils.parser import HttpParser
from threading import Thread
import socket
import datetime as dt
import time
import os
from utils.logger import create_log_msg

P_NAME = 'Response Handler'


class ResponseHandler(Thread):
    def __init__(self, i, code, pending_req_queue, log_queue):
        super(ResponseHandler, self).__init__()

        self.worker_id = i
        self.code = code
        self.queue = pending_req_queue
        self.end = False
        self.log_queue = log_queue

    def run(self):

        self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                          'Started', dt.datetime.now().strftime(
                                          '%Y/%m/%d %H:%M:%S.%f'), ''))

        while not self.end:

            p = self.queue.get()

            if p == "end":
                self.end = True
                continue

            c_sock = p[0]
            db_sock = p[1]

            self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                              'Running', dt.datetime.now().strftime(
                                              '%Y/%m/%d %H:%M:%S.%f'),
                                              'New Response from DB'))

            try:
                r_size = db_sock.recv_f(8)
                result = db_sock.recv_f(int(r_size))

                result = HttpParser.generate_response('200 OK', result)
                c_sock.send(result)

                self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                                  'Running', dt.datetime.now().strftime(
                                                  '%Y/%m/%d %H:%M:%S.%f'),
                                                  'Response sent to client'))

            finally:

                if db_sock:
                    db_sock.shutdown(socket.SHUT_RDWR)
                    db_sock.close()

                if c_sock:
                    c_sock.shutdown(socket.SHUT_RDWR)
                    c_sock.close()

        self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                          'Finished', dt.datetime.now().strftime(
                '%Y/%m/%d %H:%M:%S.%f'), ''))
