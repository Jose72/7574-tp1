from threading import Thread
from database.processing import DBProcessRequest
from utils.logger import MsgLogger
import datetime as dt
import os
from utils.protocol import ServerDBProtocol
from database.constants import LOG_DATE_FORMAT

P_NAME = 'DB Request Handler'


class DBRequestHandler(Thread):

    def __init__(self, i, server_socket, code, shard_size, file_folder, file_manager, log_queue, end_queue):

        super(DBRequestHandler, self).__init__()

        self.worker_id = i
        self.proto = ServerDBProtocol(server_socket)
        self.code = code
        self.shard_size = shard_size
        self.file_folder = file_folder
        self.file_manager = file_manager
        self.processor = DBProcessRequest(self.file_folder, self.shard_size, self.file_manager)
        self.msg_logger = MsgLogger(log_queue)
        self.end_queue = end_queue

    def run(self):

        self.msg_logger.log_msg(os.getpid(), P_NAME, self.code, 'Started', dt.datetime.now(), '')

        while self.end_queue.empty():

            # try to get a new client
            # continue if the timeout was off
            (c_proto, c_address) = self.proto.accept()
            if c_proto is None:
                continue

            # get request
            request = c_proto.receive()
            if request is None:
                self.msg_logger.log_msg(os.getpid(), P_NAME, self.code, 'Running',
                                        dt.datetime.now(), 'Connection lost at request - client:' + str(c_address))
                continue

            self.msg_logger.log_msg(os.getpid(), P_NAME, self.code, 'Running',
                                    dt.datetime.now(), 'Request received - client:' + str(c_address))

            # process request
            res = self.processor.process(self.code, request)

            # send response to client
            r_sent = c_proto.send(res)

            if r_sent is None:
                self.msg_logger.log_msg(os.getpid(), P_NAME, self.code, 'Running',
                                        dt.datetime.now(), 'Connection lost at response - client:' + str(c_address))
            else:
                self.msg_logger.log_msg(os.getpid(), P_NAME, self.code, 'Running',
                                        dt.datetime.now(), 'Response sent - client:' + str(c_address))

        self.msg_logger.log_msg(os.getpid(), P_NAME, self.code, 'Finished',
                                dt.datetime.now(), '')

        self.proto.close()

