import json
import signal
from server.server_m import Server
from utils.logger import Logger
from multiprocessing import Queue


class Post:

    def __init__(self, config_file):
        with open(config_file) as f:
            config_info_post = json.load(f)
        self.log_queue = Queue()
        self.logger = Logger('./server_post_log.txt', self.log_queue)
        self.post_server = Server(config_info_post, 'POST', self.log_queue)

    def run(self):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

        self.logger.start()
        self.post_server.start()

        signal.signal(signal.SIGINT, self.graceful_quit)
        signal.signal(signal.SIGTERM, self.graceful_quit)

        signal.pause()

    def graceful_quit(self, signum, frame):
        self.post_server.join()
        self.log_queue.put("end")
        self.logger.join()


if __name__ == "__main__":
    post = Post('./config_post.json')
    post.run()
