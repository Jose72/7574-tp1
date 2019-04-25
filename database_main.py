import json
import signal
from multiprocessing import Queue
from database.db_server import DBServer
from utils.logger import Logger


class Database:

    def __init__(self, config_file):
        with open(config_file) as f:
            config_info = json.load(f)
        self.log_queue = Queue()
        self.logger = Logger('./database_log.txt', self.log_queue)
        self.db_server = DBServer(config_info, self.log_queue)

    def run(self):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

        self.logger.start()
        self.db_server.start()

        signal.signal(signal.SIGINT, self.graceful_quit)
        signal.signal(signal.SIGTERM, self.graceful_quit)

        signal.pause()

    def graceful_quit(self, signum, frame):
        self.db_server.join()
        self.log_queue.put("end")
        self.logger.join()


if __name__ == "__main__":
    db = Database('./db_config.json')
    db.run()
