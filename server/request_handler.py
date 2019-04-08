import threading
from utils.parser import HttpParser
from utils.socket import ClientSocket, ServerSocket
from multiprocessing import Queue, Process


class RequestHandler(Process):

    def __init__(self, i, server_socket, code, db_info):

        super(RequestHandler, self).__init__()

        self.worker_id = i
        self.sock = server_socket
        self.code = code
        self.end = False
        self.subprocess_queue = Queue()
        self.subprocess = ResponseHandler(self.worker_id, self.code, self.subprocess_queue, self.end)
        self.db_ip = db_info[0]
        self.db_port = db_info[1]

        print(str(self.code) + ' Handler process: ' + str(self.worker_id) + ' - Init')

    def run(self):

        try:
            self.subprocess.start()

            print(str(self.code) + ' Handler process: ' + str(self.worker_id) + ' - Started')

            while not self.end:
                (c_fd, addr) = self.sock.accept()

                c_sock = ServerSocket()
                c_sock.move(c_fd)

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
                    self.subprocess_queue.put([c_sock, db_sock])

                    print(str(self.code) + " Response Handler: " + str(self.worker_id) + ' - Data: ' +
                        str(HttpParser.parse(self.code, request)))
                else:
                    print(str(self.code) + " Response Handler: " + str(self.worker_id) + ' - Wrong request: ')

            self.subprocess_queue.put("end")
            self.subprocess.join()
            self.sock.close()
            print(str(self.code) + ' Handler process: ' + str(self.worker_id) + ' - Finished')

        except KeyboardInterrupt:
            print(str(self.code) + ' Handler process: ' + str(self.worker_id) + ' - Interrupted')
            self.subprocess_queue.put("end")
            self.subprocess.join()
            self.sock.close()

    def finish(self):
        self.end = True


class ResponseHandler(threading.Thread):

    def __init__(self, i, code, queue, end):
        super(ResponseHandler, self).__init__()

        self.worker_id = i
        self.code = code
        self.queue = queue
        self.end = end

    def run(self):
        print(str(self.code) + " Response Handler: " + str(self.worker_id) + " - Started")

        while not self.end:

            p = self.queue.get()

            if p == "end":
                print(str(self.code) + " Response Handler: " + str(self.worker_id) + " - Finished")
                return

            c_sock = p[0]
            db_sock = p[1]

            r_size = db_sock.recv_f(4)
            result = db_sock.recv_f(int(r_size))
            db_sock.close()

            result = HttpParser.generate_response('200 OK', result)
            print(str(self.code) + " Response Handler: " + str(self.worker_id) + ' - Data: \n' + str(result))
            c_sock.send(result)

            c_sock.close()
