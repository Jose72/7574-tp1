import sys
import os
import json
from os import path
from server.server_m import Server


def main():
    with open('./config_get.json') as f:
        config_info_get = json.load(f)
    with open('./config_post.json') as f:
        config_info_post = json.load(f)
    get_serv = Server(config_info_get, 'GET')
    post_serv = Server(config_info_post, 'POST')
    print(str(os.getpid()) + "Main process")
    print(str(os.getpid()) + "Main Started")
    post_serv.start()
    get_serv.start()


if __name__ == "__main__":
    main()


