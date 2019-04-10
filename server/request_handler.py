from utils.parser import HttpParser
from utils.socket import ClientSocket, ServerSocket
from multiprocessing import Process
from server.response_handler import ResponseHandler
from threading import Thread
import multiprocessing
import signal
import socket
import time


class RequestHandler(Process):

    def __init__(self, i, server_socket, code, db_info, max_req):

        super(RequestHandler, self).__init__()

        self.worker_id = i
        self.sock = server_socket
        self.code = code
        self.end = False
        self.max_req = max_req
        self.pending_req_queue = multiprocessing.Queue(maxsize=int(self.max_req))
        self.db_ip = db_info[0]
        self.db_port = db_info[1]
        self.response_handler = ResponseHandler(i, self.code, self.pending_req_queue)

        print(str(self.code) + ' Handler process: ' + str(self.worker_id) + ' - Init')

    def run(self):

        # signal.signal(signal.SIGINT, signal.SIG_IGN)

        db_sock = None
        c_sock = None

        self.response_handler.start()

        try:

            print(str(self.code) + ' Handler process: ' + str(self.worker_id) + ' - Started')

            while not self.end:
                (c_fd, addr) = self.sock.accept()

                c_sock = ServerSocket()
                c_sock.move_from(c_fd)

                request = c_sock.recv(4096)
                print("request : " + str(request))

                if HttpParser.check_correct_service(request, self.code, '/log'):

                    # check if the queue is full first
                    # if not send to db server
                    db_sock = ClientSocket()
                    db_sock.connect(self.db_ip, self.db_port)
                    db_msg = HttpParser.parse(self.code, request)
                    db_sock.send(str(len(db_msg)).zfill(8))
                    db_sock.send(db_msg)

                    self.pending_req_queue.put([c_sock, db_sock])

                    print(str(self.code) + " Response Handler: " + str(self.worker_id) + ' - Data: ' +
                          str(HttpParser.parse(self.code, request)))
                else:
                    c_sock.close()
                    print(str(self.code) + " Response Handler: " + str(self.worker_id) + ' - Wrong request: ')

            self.pending_req_queue.put("end")
            self.sock.close()
            print(str(self.code) + ' Handler process: ' + str(self.worker_id) + ' - Finished')

        except KeyboardInterrupt:
            self.end = True
            self.pending_req_queue.put("end")
            print(str(self.code) + ' Handler process: ' + str(self.worker_id) + ' - Interrupted')

        finally:
            self.sock.close()
            self.response_handler.join()




