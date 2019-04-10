from multiprocessing import Process, Queue
from utils.socket import ServerSocket
from server.request_handler import RequestHandler
from server.response_handler import ResponseHandler
from pprint import pprint


class Server(Process):

    def __init__(self, config_info, code):

        super(Server, self).__init__()

        pprint(config_info)

        self.ip_address = config_info['ip_address']
        self.port = config_info['port']
        self.max_con = config_info['max_con']
        self.workers_n = config_info['workers_n']
        self.code = code
        self.request_process_pool = []
        self.sock = ServerSocket()
        self.sock.init(self.ip_address, self.port, self.max_con)
        self.db_ip = config_info['db_ip']
        self.db_port = config_info['db_port']
        self.max_requests = config_info['max_requests']

        self.end = False
        print(str(self.code) + ' Server: Init')

    def run(self):

        try:
            print(str(self.code) + ' Server: Running')

            # Create request workers
            for i in range(0, self.workers_n):
                w = RequestHandler(i, self.sock, self.code, (self.db_ip, self.db_port), (self.max_requests / self.workers_n))
                self.request_process_pool.append(w)
                w.start()

            # Wait for workers to finish
            for i in range(0, self.workers_n):
                self.request_process_pool[i].join()

            # Close the socket
            self.sock.close()

        except KeyboardInterrupt:

            self.end = True
            print(str(self.code) + ' Server: Interrupted')

        finally:

            # Wait for request workers to finish
            for p in self.request_process_pool:
                p.join()

            # Close the socket
            self.sock.close()

            print(str(self.code) + ' Server: Exiting')
