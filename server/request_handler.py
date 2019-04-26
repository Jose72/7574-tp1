from utils.parser import HttpParser
from utils.socket import ClientSocket
from multiprocessing import Process
from server.response_handler import ResponseHandler
import multiprocessing
import datetime as dt
import os
import signal
from utils.protocol import ClientServerProtocol, ServerDBProtocol
from utils.logger import MsgLogger


P_NAME = 'Request Handler'


class RequestHandler(Process):

    def __init__(self, i, server_socket, code, db_info, max_req, log_queue, end_queue):

        super(RequestHandler, self).__init__()

        self.worker_id = i
        self.proto = ClientServerProtocol(server_socket)
        self.code = code
        self.max_req = max_req
        self.pending_req_queue = multiprocessing.Queue(maxsize=int(self.max_req))
        self.db_ip = db_info[0]
        self.db_port = db_info[1]
        self.msg_logger = MsgLogger(log_queue)
        self.response_handler = ResponseHandler(i, self.code, self.pending_req_queue, log_queue)
        self.end_queue = end_queue

    def run(self):

        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

        self.msg_logger.log_msg(os.getpid(), P_NAME, self.code, 'Started', dt.datetime.now(), '')

        # Start the response handler thread
        self.response_handler.start()

        while self.end_queue.empty():

            # try to get a new client
            (c_proto, c_address) = self.proto.accept()
            if c_proto is None:
                continue

            # try to get a request
            request = c_proto.receive()
            if request is None:
                self.msg_logger.log_msg(os.getpid(), P_NAME, self.code, 'Running',
                                        dt.datetime.now(), 'Connection lost at request - client:' + str(c_address))
                continue

            self.msg_logger.log_msg(os.getpid(), P_NAME, self.code, 'Running',
                                    dt.datetime.now(), 'Request received - client:' + str(c_address))

            # If the the service is the right one (its a get/post and correct url)
            correct_service = HttpParser.check_correct_service(request, self.code, '/log')

            # TO DO: check if the request is valid (right format, valid json fields, etc)

            # if correct service and queue is not full
            # send to db server
            if correct_service and not self.pending_req_queue.full():

                # connect with db and send request
                db_proto = ServerDBProtocol(ClientSocket())
                db_proto.connect(self.db_ip, self.db_port)
                db_msg = HttpParser.parse(self.code, request)
                sent = db_proto.send(db_msg)
                if sent is None:
                    self.msg_logger.log_msg(os.getpid(), P_NAME, self.code, 'Running',
                                            dt.datetime.now(), 'Service Unavailable - client:' + str(c_address))
                    res_error = HttpParser.generate_response('503 Service Unavailable', '')
                    c_proto.send(res_error)
                    c_proto.close()
                    continue

                self.pending_req_queue.put([c_proto, db_proto, c_address])

            else:
                # if request was not valid
                # return error msg to client
                self.msg_logger.log_msg(os.getpid(), P_NAME, self.code, 'Running',
                                        dt.datetime.now(), 'Bad Request - client:' + str(c_address))
                res_error = HttpParser.generate_response('400 Bad Request', '')
                c_proto.send(res_error)
                c_proto.shutdown()
                c_proto.close()

        self.pending_req_queue.put("end")
        self.proto.close()
        self.response_handler.join()
        self.msg_logger.log_msg(os.getpid(), P_NAME, self.code, 'Finished',
                                dt.datetime.now(), '')
