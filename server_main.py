import os
import json
from server.server_m import Server
from utils.logger import Logger
from multiprocessing import Queue
from time import sleep


def main():
    with open('./config_get.json') as f:
        config_info_get = json.load(f)
    with open('./config_post.json') as f:
        config_info_post = json.load(f)
    log_queue = Queue()
    logger = Logger('./server_log.txt', log_queue)
    get_server = Server(config_info_get, 'GET', log_queue)
    post_server = Server(config_info_post, 'POST', log_queue)
    logger.start()
    post_server.start()
    get_server.start()

    try:
        while True:
            sleep(60)

    except KeyboardInterrupt:
        post_server.join()
        get_server.join()
        log_queue.put("end")
        logger.join()


if __name__ == "__main__":
    main()


