from utils.parser import HttpParser
from utils.socket import ClientSocket, ServerSocket
from multiprocessing import Process
from server.response_handler import ResponseHandler
from threading import Thread
import multiprocessing
import signal
import socket
import datetime as dt
import os
import time
from utils.logger import create_log_msg

P_NAME = 'Request Handler'


class RequestHandler(Process):

    def __init__(self, i, server_socket, code, db_info, max_req, log_queue):

        super(RequestHandler, self).__init__()

        self.worker_id = i
        self.sock = server_socket
        self.code = code
        self.end = False
        self.max_req = max_req
        self.pending_req_queue = multiprocessing.Queue(maxsize=int(self.max_req))
        self.db_ip = db_info[0]
        self.db_port = db_info[1]
        self.log_queue = log_queue
        self.response_handler = ResponseHandler(i, self.code, self.pending_req_queue, log_queue)

    def run(self):

        self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                          'Started', dt.datetime.now().strftime(
                '%Y/%m/%d %H:%M:%S.%f'), ''))

        # Start the response handler thread
        self.response_handler.start()

        try:

            while not self.end:

                # Accept a new connection
                (c_fd, addr) = self.sock.accept()

                c_sock = ServerSocket()
                c_sock.move_from(c_fd)

                # Receive the http package
                request = c_sock.recv(4096)

                # If the the service is the right one (its a get/post and correct url)
                correct_service = HttpParser.check_correct_service(request, self.code, '/log')

                # TO DO: check if the request is valid (right format, valid json fields, etc)

                # set an error response for later
                res_error = HttpParser.generate_response('503 Service Unavailable', '')

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
                        c_sock.send(res_error)
                        c_sock.close()

                else:
                    # if request was not valid
                    # return error msg to client
                    c_sock.send(res_error)
                    c_sock.close()

        except KeyboardInterrupt:
            self.end = True
            self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                              'Interrupted', dt.datetime.now().strftime(
                                              '%Y/%m/%d %H:%M:%S.%f'), ''))

        finally:
            # Send message to end the thread and close the accepting socket
            self.pending_req_queue.put("end")
            self.sock.close()
            self.response_handler.join()
            self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                              'Finished', dt.datetime.now().strftime(
                                              '%Y/%m/%d %H:%M:%S.%f'), ''))




