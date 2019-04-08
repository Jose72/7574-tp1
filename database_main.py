import os
import json
from database.db_server import DBServer


def main():
    with open('./db_config.json') as f:
        config_info = json.load(f)
    db_server = DBServer(config_info)
    print(str(os.getpid()) + "DB Main Started")
    db_server.start()


if __name__ == "__main__":
    main()
