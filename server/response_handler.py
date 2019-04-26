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

            c_proto = p[0]
            db_proto = p[1]

            self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                              'Running', dt.datetime.now().strftime(
                                              '%Y/%m/%d %H:%M:%S.%f'),
                                              'New Response from DB'))

            # get result from db
            result = db_proto.receive()

            if result is None:
                self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                                  'Running', dt.datetime.now().strftime(
                        '%Y/%m/%d %H:%M:%S.%f'), 'DB connection lost'))
                res_error = HttpParser.generate_response('503 Service Unavailable', '')
                c_proto.send(res_error)
                c_proto.close()

            # send result to client
            result = HttpParser.generate_response('200 OK', result)

            c_proto.send(result)

            self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                              'Running', dt.datetime.now().strftime(
                    '%Y/%m/%d %H:%M:%S.%f'),
                                              'Response sent to client'))

            db_proto.shutdown()
            db_proto.close()

            c_proto.shutdown()
            c_proto.close()

        self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                          'Finished', dt.datetime.now().strftime(
                '%Y/%m/%d %H:%M:%S.%f'), ''))
