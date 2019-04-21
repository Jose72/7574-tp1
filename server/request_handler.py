from utils.parser import HttpParser
from utils.socket import ClientSocket, Socket
from multiprocessing import Process
from server.response_handler import ResponseHandler
import multiprocessing
import socket
import datetime as dt
import os
from utils.logger import create_log_msg

P_NAME = 'Request Handler'


class RequestHandler(Process):

    def __init__(self, i, server_socket, code, db_info, max_req, log_queue):

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

    def run(self):

        self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                          'Started', dt.datetime.now().strftime(
                '%Y/%m/%d %H:%M:%S.%f'), ''))

        # Start the response handler thread
        self.response_handler.start()

        try:

            while True:

                # Accept a new connection
                (c_fd, addr) = self.sock.accept()

                c_sock = Socket()
                c_sock.move_from(c_fd)

                # Receive the http package
                request = c_sock.recv(4096)

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

        except KeyboardInterrupt:
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
