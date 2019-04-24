from utils.parser import HttpParser
from utils.socket import ClientSocket, Socket
from multiprocessing import Process, Queue
from server.response_handler import ResponseHandler
import multiprocessing
import socket
import datetime as dt
import os
import signal
from queue import Empty
from utils.protocol import AcceptProtocol
from utils.logger import create_log_msg

P_NAME = 'Request Handler'


class RequestHandler(Process):

    def __init__(self, i, server_socket, code, db_info, max_req, log_queue, end_queue):

        super(RequestHandler, self).__init__()

        self.worker_id = i
        self.sock = server_socket
        self.code = code
        self.max_req = max_req
        self.pending_req_queue = multiprocessing.Queue(maxsize=int(self.max_req))
        self.db_ip = db_info[0]
        self.db_port = db_info[1]
        self.log_queue = log_queue
        self.response_handler = ResponseHandler(i, self.code, self.pending_req_queue, log_queue)
        self.end = False
        self.end_queue = end_queue

    def run(self):

        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

        self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                          'Started', dt.datetime.now().strftime(
                '%Y/%m/%d %H:%M:%S.%f'), ''))

        # Start the response handler thread
        self.response_handler.start()

        while not self.end:

            # try to get a new client/request
            # if the timeout was off check for end message
            # continue if its not the end
            try:
                a_prot = AcceptProtocol(self.sock)
                (c_sock, addr, request) = a_prot.get_client_and_request()
            except socket.error:
                try:
                    self.end = self.end_queue.get(timeout=0.2)
                except Empty:
                    continue
                continue

            self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                              'Running', dt.datetime.now().strftime(
                    '%Y/%m/%d %H:%M:%S.%f'), 'Request Received - client:' + str(addr)))

            # If the the service is the right one (its a get/post and correct url)
            correct_service = HttpParser.check_correct_service(request, self.code, '/log')

            # TO DO: check if the request is valid (right format, valid json fields, etc)

            # if correct service and queue is not full
            # send to db server
            if correct_service and not self.pending_req_queue.full():

                try:
                    # connect with db and send request
                    db_sock = ClientSocket()
                    db_sock.connect(self.db_ip, self.db_port)
                    db_msg = HttpParser.parse(self.code, request)
                    db_sock.send(str(len(db_msg)).zfill(8))
                    db_sock.send(db_msg)
                    self.pending_req_queue.put([c_sock, db_sock])

                except ConnectionRefusedError:
                    # if error in sending request to db
                    # return error msg to client
                    self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                                      'Running', dt.datetime.now().strftime(
                            '%Y/%m/%d %H:%M:%S.%f'), 'Service Unavailable - client:' + str(addr)))
                    res_error = HttpParser.generate_response('503 Service Unavailable', '')
                    c_sock.send(res_error)
                    c_sock.close()

            else:
                # if request was not valid
                # return error msg to client
                self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                                  'Running', dt.datetime.now().strftime(
                        '%Y/%m/%d %H:%M:%S.%f'), 'Bad Request - client:' + str(addr)))
                res_error = HttpParser.generate_response('400 Bad Request', '')
                c_sock.send(res_error)
                c_sock.shutdown(socket.SHUT_RDWR)
                c_sock.close()

        self.pending_req_queue.put("end")
        self.sock.close()
        self.response_handler.join()
        self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                          'Finished', dt.datetime.now().strftime(
                '%Y/%m/%d %H:%M:%S.%f'), ''))

    def graceful_quit(self):
        self.end = True
