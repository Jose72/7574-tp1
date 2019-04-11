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

                # f.write('------------------------------------------------------')

                # f.write(create_log_msg(os.getpid(), P_NAME, 0,
                #                       'Started', dt.datetime.now().strftime(
                #                        '%Y/%m/%d %H:%M:%S.%f'), ''))

        end = False
        while not end:
            msg = self.msg_queue.get()
            if msg == "end":
                end = True
                continue
            with open(self.file, 'a+') as f:
                f.write(str(msg))
                f.close()

                #f.write(create_log_msg(os.getpid(), P_NAME, 0,
                #                       'Finished', dt.datetime.now().strftime(
                #        '%Y/%m/%d %H:%M:%S.%f'), ''))




def create_log_msg(pid, process_name, process_code, status, timestamp, msg):
    return 'pid:{} - {} - {} - status:{} - {} - {} \n'.format(pid, process_name, process_code, status, timestamp, msg)
