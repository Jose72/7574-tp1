from utils.parser import HttpParser
from threading import Thread
import datetime as dt
import os
from utils.logger import MsgLogger

P_NAME = 'Response Handler'


class ResponseHandler(Thread):
    def __init__(self, i, code, pending_req_queue, log_queue):
        super(ResponseHandler, self).__init__()

        self.worker_id = i
        self.code = code
        self.queue = pending_req_queue
        self.end = False
        self.msg_logger = MsgLogger(log_queue)

    def run(self):

        self.msg_logger.log_msg(os.getpid(), P_NAME, self.code, 'Started', dt.datetime.now(), '')

        while not self.end:

            p = self.queue.get()

            if p == "end":
                self.end = True
                continue

            c_proto = p[0]
            db_proto = p[1]
            c_address = p[2]

            self.msg_logger.log_msg(os.getpid(), P_NAME, self.code, 'Running',
                                    dt.datetime.now(), 'New response from DB')

            # get result from db
            result = db_proto.receive()

            # if connection lost with db
            # send error to client and close connection
            if result is None:
                self.msg_logger.log_msg(os.getpid(), P_NAME, self.code, 'Running',
                                        dt.datetime.now(), 'DB connection lost:')
                res_error = HttpParser.generate_response('503 Service Unavailable', '')
                c_proto.send(res_error)
                c_proto.close()
                continue

            # send result to client
            result = HttpParser.generate_response('200 OK', result)

            sent_ok = c_proto.send(result)
            if sent_ok is None:
                self.msg_logger.log_msg(os.getpid(), P_NAME, self.code, 'Running',
                                        dt.datetime.now(), 'Connection lost at response - client:' + str(c_address))
            else:
                self.msg_logger.log_msg(os.getpid(), P_NAME, self.code, 'Running',
                                        dt.datetime.now(), 'Response sent - client:' + str(c_address))

            db_proto.shutdown()
            db_proto.close()

            c_proto.shutdown()
            c_proto.close()

        self.msg_logger.log_msg(os.getpid(), P_NAME, self.code, 'Finished',
                                dt.datetime.now(), '')
