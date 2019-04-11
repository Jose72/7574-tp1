import os
import sys
import json
from time import sleep
from multiprocessing import Queue
from database.db_server import DBServer
from utils.logger import Logger


def main():
    with open('./db_config.json') as f:
        config_info = json.load(f)
    log_queue = Queue()
    logger = Logger('./database_log.txt', log_queue)
    db_server = DBServer(config_info, log_queue)
    logger.start()
    db_server.start()

    try:
        while True:
            sleep(60)

    except KeyboardInterrupt:
        db_server.join()
        log_queue.put("end")
        logger.join()


if __name__ == "__main__":
    main()
