import json
import signal
from server.server_m import Server
from utils.logger import Logger
from multiprocessing import Queue


def main():
    with open('./config_get.json') as f:
        config_info_get = json.load(f)
    log_queue = Queue()
    logger = Logger('./server_get_log.txt', log_queue)
    get_server = Server(config_info_get, 'GET', log_queue)
    logger.start()
    get_server.start()

    try:
        while True:
            signal.pause()

    except KeyboardInterrupt:
        get_server.join()
        log_queue.put("end")
        logger.join()


if __name__ == "__main__":
    main()