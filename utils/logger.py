from multiprocessing import Process
import signal
import datetime as dt
import os

P_NAME = 'Logger'


class Logger(Process):

    def __init__(self, file_path, l_queue):
        super(Logger, self).__init__()
        self.msg_queue = l_queue
        self.file = file_path

    def run(self):

        signal.signal(signal.SIGINT, signal.SIG_IGN)

        end = False
        while not end:
            msg = self.msg_queue.get()
            if msg == "end":
                end = True
                continue
            with open(self.file, 'a+') as f:
                f.write(str(msg))
                f.close()


class MsgLogger:

    def __init__(self, log_queue):
        self.log_queue = log_queue

    def log_msg(self, pid, process_name, process_code, status, timestamp, msg):
        self.log_queue.put(create_log_msg(
            pid, process_name, process_code,
            status, timestamp.strftime('%Y/%m/%d %H:%M:%S.%f'),
            msg))


def create_log_msg(pid, process_name, process_code, status, timestamp, msg):
    return 'pid:{} - {} - {} - status:{} - {} - {} \n'.format(pid, process_name, process_code, status, timestamp, msg)
