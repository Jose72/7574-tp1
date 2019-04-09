import threading
from utils.parser import HttpParser
from utils.socket import ClientSocket, ServerSocket
from multiprocessing import Process


class RequestHandler(Process):

    def __init__(self, i, server_socket, code, db_info, p_r_queue):

        super(RequestHandler, self).__init__()

        self.worker_id = i
        self.sock = server_socket
        self.code = code
        self.end = False
        self.pending_req_queue = p_r_queue
        # self.subprocess = ResponseHandler(self.worker_id, self.code, self.subprocess_queue, self.end)
        self.db_ip = db_info[0]
        self.db_port = db_info[1]

        print(str(self.code) + ' Handler process: ' + str(self.worker_id) + ' - Init')

    def run(self):

        try:
            #self.subprocess.start()

            print(str(self.code) + ' Handler process: ' + str(self.worker_id) + ' - Started')

            while not self.end:
                (c_fd, addr) = self.sock.accept()

                c_sock = ServerSocket()
                c_sock.move_from(c_fd)

                request = c_sock.recv(1024)
                print("request : " + str(request))

                if HttpParser.check_correct_service(request, self.code, '/log'):

                    # send to db
                    db_sock = ClientSocket()
                    db_sock.connect(self.db_ip, self.db_port)
                    db_msg = HttpParser.parse(self.code, request)
                    db_sock.send(str(len(db_msg)).zfill(4))
                    db_sock.send(db_msg)

                    # send sockets to response handler
                    self.pending_req_queue.put([c_sock, db_sock])

                    print(str(self.code) + " Response Handler: " + str(self.worker_id) + ' - Data: ' +
                          str(HttpParser.parse(self.code, request)))
                else:
                    print(str(self.code) + " Response Handler: " + str(self.worker_id) + ' - Wrong request: ')

            self.pending_req_queue.put("end")
            self.sock.close()
            print(str(self.code) + ' Handler process: ' + str(self.worker_id) + ' - Finished')

        except KeyboardInterrupt:
            print(str(self.code) + ' Handler process: ' + str(self.worker_id) + ' - Interrupted')
            self.pending_req_queue.put("end")
            self.sock.close()
