import json
import signal
from server.server_m import Server
from utils.logger import Logger
from multiprocessing import Queue


def main():
    with open('./config_post.json') as f:
        config_info_post = json.load(f)
    log_queue = Queue()
    logger = Logger('./server_post_log.txt', log_queue)
    post_server = Server(config_info_post, 'POST', log_queue)
    logger.start()
    post_server.start()

    try:
        while True:
            signal.pause()

    except KeyboardInterrupt:
        post_server.join()
        log_queue.put("end")
        logger.join()


if __name__ == "__main__":
    main()
